import os
import math
import time
import logging
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from obs import ObsClient, CompleteMultipartUploadRequest, CompletePart, ListMultipartUploadsRequest
from tqdm import tqdm
import http.client
# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ObsStream")

# ==========================================
# 🩹 猴子补丁 (Monkey Patch)
# 修复 huaweicloud-sdk-python-obs SSL 连接参数报错问题
# ==========================================
def _patched_get_server_connection(self, is_secure, server, port, proxy_host, proxy_port):
    """覆盖 SDK 原有的连接创建方法，移除不支持的 check_hostname 参数"""
    if proxy_host is not None and proxy_port is not None:
        server = proxy_host
        port = proxy_port

    if is_secure:
        # 关键修改：直接移除 check_hostname 参数
        # Python 3 的 HTTPSConnection 会自动使用 context 中的配置
        try:
            conn = http.client.HTTPSConnection(
                server,
                port=port,
                timeout=self.timeout,
                context=self.context
            )
        except TypeError:
            # 兜底：万一 context 也不支持（极少见），则不传 context
            conn = http.client.HTTPSConnection(
                server,
                port=port,
                timeout=self.timeout
            )
    else:
        conn = http.client.HTTPConnection(server, port=port, timeout=self.timeout)

    return conn

# 应用补丁：替换 ObsClient 类的内部方法
ObsClient._get_server_connection_use_http1x = _patched_get_server_connection
# ==========================================
# 🩹 猴子补丁 结束
# ==========================================

class UploadContext:
    """
    上传上下文：用于在初始化和实际上传之间传递状态
    """

    def __init__(self, object_key, upload_id, offset, next_part):
        self.key = object_key
        self.upload_id = upload_id  # OBS 内部任务 ID (用于续传)
        self.offset = offset  # 已上传字节数 (用于告诉下载器 Range)
        self.next_part = next_part  # 下一个分片号 (用于分片排序)


class StreamUploader:
    """
    华为云 OBS 流式上传工具 (支持断点续传)
    """

    def __init__(self, ak=None, sk=None, server=None, bucket_name=None, part_size=20 * 1024 * 1024):
        # 优先从环境变量读取配置，支持无参初始化
        self.ak = ak or os.getenv("OBS_AK")
        self.sk = sk or os.getenv("OBS_SK")
        self.server = server or os.getenv("OBS_SERVER")
        self.bucket = bucket_name or os.getenv("OBS_BUCKET")

        if not all([self.ak, self.sk, self.server, self.bucket]):
            raise ValueError("必须提供 AK, SK, Server 和 Bucket Name (可通过参数或环境变量)")

        self.client = ObsClient(access_key_id=self.ak, secret_access_key=self.sk, server=self.server)

        # --- 核心配置 ---
        # 分片大小默认 20MB，既能保证并发度，又适配流式场景
        self.part_size = part_size
        self.max_workers = 5  # 并发上传线程数
        self.max_retries = 5  # 单个分片上传失败重试次数

    def init_upload(self, object_key):
        """
        【第一步】初始化上传任务，探测断点
        :param object_key: OBS 目标路径
        :return: UploadContext 对象
        """
        logger.info(f"正在初始化任务: {object_key} ...")

        # 1. 去 OBS 查有没有未完成的任务
        upload_id, uploaded_bytes, next_part = self._get_resume_info(object_key)

        if upload_id:
            logger.info(f"[断点发现] 任务ID: {upload_id}, "
                        f"已上传: {uploaded_bytes / 1024 / 1024:.2f} MB, "
                        f"parts: {next_part}")
            logger.info(f"-> 从 offset={uploaded_bytes} 处开始下载")
        else:
            # 没有断点，初始化一个新任务
            resp = self.client.initiateMultipartUpload(self.bucket, object_key)
            self._check_error(resp)
            upload_id = resp.body.uploadId
            uploaded_bytes = 0
            next_part = 1
            logger.info(f"[新任务] 已创建任务ID: {upload_id}")

        # 打包上下文返回
        return UploadContext(object_key, upload_id, uploaded_bytes, next_part)

    def upload_stream(self, context, stream_iterator, total_size=None, mode="ab"):
        """
        【第二步】接收数据流，执行并发分片上传
        :param context: init_upload 返回的 UploadContext 对象
        :param stream_iterator: 数据流 (bytes 生成器)
        :param total_size: (可选) 剩余文件大小，用于进度条显示
        :param mode: "ab" 代表追加(续传)，"wb" 代表覆盖(重传)
        :return: 文件在 OBS 上的总大小 (int) -> (旧offset + 本次上传量)
        """
        if mode == "wb":
            logger.info(f"模式为 wb，正在清理并重置任务: {context.key}")
            # 1. 销毁旧任务
            try:
                self.client.abortMultipartUpload(self.bucket, context.key, context.upload_id)
            except Exception:
                pass
            
            # 2. 开启新任务并更新上下文
            resp = self.client.initiateMultipartUpload(self.bucket, context.key)
            self._check_error(resp)
            context.upload_id = resp.body.uploadId
            context.offset = 0
            context.next_part = 1

        logger.info(f"开始接收数据流，写入: {context.key} (Mode: {mode}, Offset: {context.offset})")

        # 动态调整分片大小
        current_part_size = self.part_size
        if total_size and total_size > 0:
            # 已知 Total Size：动态计算最小分片大小
            # OBS 分片数量上限为 10000。这里除以 9000 预留安全 Buffer，防止边缘溢出
            # 效果：文件 < 180GB 时，计算结果 < 20MB，继续使用默认 20MB（小文件省内存）
            #      文件 > 180GB 时，计算结果 > 20MB，自动增大分片确保不超限
            min_part_size = math.ceil(total_size / 9000)
            if min_part_size > current_part_size:
                if context.offset == 0:
                    logger.warning(f"文件大小 ({total_size / 1024 / 1024 / 1024:.2f} GB) 超过当前分片限制，"
                                   f"自动调整分片大小: {current_part_size / 1024 / 1024:.0f}MB -> {min_part_size / 1024 / 1024:.0f}MB")
                    current_part_size = min_part_size
                else:
                    logger.error(f"文件过大导致分片数可能超过 10000 上限 (需 {min_part_size / 1024 / 1024:.0f}MB/part)，"
                                 f"但当前处于续传模式 (使用 {current_part_size / 1024 / 1024:.0f}MB/part)。"
                                 f"请使用 mode='wb' 重新开始上传。")
        else:
            # 未知 Total Size (stream 模式)：强制使用 150MB 分片
            # 原因：无法预知大小，为安全起见默认给大一点，可以上传超过 1TB 文件
            # 150MB * 9000 ≈ 1.35TB，足够覆盖绝大多数场景
            stream_default_part_size = 150 * 1024 * 1024  # 150MB
            if current_part_size < stream_default_part_size:
                logger.info(f"未知文件大小（流模式），使用默认大分片: {stream_default_part_size / 1024 / 1024:.0f}MB")
                current_part_size = stream_default_part_size

        try:
            # 执行核心上传逻辑
            bytes_uploaded = self._process_stream(
                stream_iterator,
                context.key,
                context.upload_id,
                context.next_part,
                total_size,
                current_part_size
            )

            # 只有流正常结束，才执行合并操作
            self._complete_upload(context.key, context.upload_id)
            
            # 返回 OBS 上的最终文件大小
            return context.offset + bytes_uploaded

        except Exception as e:
            logger.error(f"上传过程中断: {e}")
            raise e

    def _get_resume_info(self, key):
        """查询 OBS 服务端是否存在未完成的分段任务"""
        # 1. 列出桶内所有分段任务
        list_req = ListMultipartUploadsRequest(prefix=key)
        resp = self.client.listMultipartUploads(self.bucket, multipart=list_req)

        target_id = None
        # 找到 key 完全匹配的任务 (取最新的一个)
        if resp.status < 300 and resp.body.upload:
            for upload in resp.body.upload:
                if upload.key == key:
                    target_id = upload.uploadId
                    # 注意：这里不 break，继续找可能是为了找最新的，或者默认取第一个匹配的
                    # 在本简版实现中，取列表中的第一个匹配项通常即可
                    break

        if not target_id:
            return None, 0, 1

        # 2. 统计该任务已上传的分片，计算 offset
        uploaded_bytes = 0
        next_part = 1
        marker = None

        # 分页拉取所有已上传分片
        while True:
            parts_resp = self.client.listParts(self.bucket, key, target_id, partNumberMarker=marker)
            if parts_resp.status >= 300:
                logger.warning(f"查询分片失败: {parts_resp.errorMessage}")
                return None, 0, 1  # 降级为新任务

            for part in parts_resp.body.parts:
                # 简单校验：假设分片是连续上传的，且大小符合当前配置
                # 如果历史分片大小和当前配置不一致 (除了最后一个)，可能导致续传错位
                if part.partNumber == next_part:
                    # 严格模式下应校验 part.size == self.part_size
                    uploaded_bytes += part.size
                    next_part += 1

            if not parts_resp.body.isTruncated:
                break
            marker = parts_resp.body.nextPartNumberMarker

        return target_id, uploaded_bytes, next_part

    def _process_stream(self, iterator, key, uid, start_part, total_size, part_size):
        """读取流 -> 缓冲 -> 提交线程池"""
        buffer = BytesIO()
        part_number = start_part
        total_stream_bytes = 0

        # 如果是续传，先拉取历史分片信息
        parts_map = {}
        if start_part > 1:
            parts_map = self._fetch_uploaded_parts_map(key, uid)

        # 进度条设置
        pbar = None
        if tqdm:
            current_uploaded = (start_part - 1) * part_size
            pbar = tqdm(
                total=total_size,
                initial=current_uploaded,
                unit='B',
                unit_scale=True,
                desc=f"🚀 Uploading {os.path.basename(key)}",
                mininterval=1,
                position=0,
                dynamic_ncols=True
            )

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {}  # {future: part_number}

            try:
                for chunk in iterator:
                    if chunk:
                        total_stream_bytes += len(chunk)
                        buffer.write(chunk)

                        if pbar is not None:
                            pbar.update(len(chunk))

                        # 缓冲区达到分片大小 -> 提交上传
                        if buffer.tell() >= part_size:
                            data = buffer.getvalue()

                            f = executor.submit(self._upload_part_with_retry, key, uid, part_number, data)
                            futures[f] = part_number

                            part_number += 1
                            buffer = BytesIO()  # 重置缓冲

                            # 流控：防止内存溢出
                            if len(futures) >= self.max_workers * 2:
                                self._wait_and_collect(futures, parts_map)

                # 处理剩余数据
                if buffer.tell() > 0:
                    f = executor.submit(self._upload_part_with_retry, key, uid, part_number, buffer.getvalue())
                    futures[f] = part_number

                # 等待所有任务完成
                for f in as_completed(futures):
                    p_num = futures[f]
                    etag = f.result()
                    parts_map[p_num] = etag

            except Exception as e:
                raise e
            finally:
                if pbar is not None:
                    pbar.close()

        # 保存分片映射供合并使用
        self._final_parts_map = parts_map
        return total_stream_bytes

    def _upload_part_with_retry(self, key, uid, p_num, data):
        """带重试机制的单个分片上传（增加详细日志）"""
        data_len = len(data)

        for i in range(self.max_retries):
            try:
                start_time = time.time()
                # 执行上传
                resp = self.client.uploadPart(
                    bucketName=self.bucket, objectKey=key, partNumber=p_num,
                    uploadId=uid, content=data, partSize=data_len
                )
                if resp.status < 300:
                    # 计算耗时和速度
                    duration = time.time() - start_time
                    speed = (data_len / 1024 / 1024) / duration if duration > 0 else 0
                    # ✅ 打印详细的成功日志
                    logger.info(f"分片 #{p_num} 上传成功 | "
                                f"大小: {data_len / 1024 / 1024:.2f}MB | "
                                f"耗时: {duration:.1f}s | "
                                f"速度: {speed:.1f}MB/s")
                    return resp.body.etag
                else:
                    logger.warning(
                        f"⚠️ 分片 #{p_num} 上传失败 (HTTP {resp.status}, Code: {resp.errorCode}, Msg: {resp.errorMessage})，正在重试 {i + 1}/{self.max_retries}...")

            except Exception as ex:
                logger.warning(f"❌ 分片 #{p_num} 发生异常: {ex}，正在重试 {i + 1}/{self.max_retries}...")
            # 失败后稍微等待一下再重试
            time.sleep(1 * (i + 1))
        raise Exception(f"分片 #{p_num} 在 {self.max_retries} 次尝试后最终失败")

    def _wait_and_collect(self, futures, parts_map):
        """等待部分任务完成，回收内存，收集 ETag"""
        done, _ = wait(futures.keys(), return_when=FIRST_COMPLETED)
        for f in done:
            p_num = futures.pop(f)
            try:
                parts_map[p_num] = f.result()
            except Exception as e:
                # 这里捕获异常是为了不打断主循环，
                # 但实际上如果分片失败，最终合并会失败，或者上面已经抛出了
                raise e

    def _fetch_uploaded_parts_map(self, key, uid):
        """获取服务端已有的分片信息 (PartNum -> ETag)"""
        mapping = {}
        marker = None
        while True:
            resp = self.client.listParts(self.bucket, key, uid, partNumberMarker=marker)
            if resp.status >= 300: break
            for p in resp.body.parts:
                mapping[p.partNumber] = p.etag
            if not resp.body.isTruncated: break
            marker = resp.body.nextPartNumberMarker
        return mapping

    def _complete_upload(self, key, uid):
        """合并分片"""
        logger.info("流传输结束，正在请求合并分片...")

        # 使用最新的 parts_map (包含历史的和本次上传的)
        # 如果 _process_stream 成功执行，self._final_parts_map 应该有完整数据
        # 为了保险，这里可以使用 _fetch_uploaded_parts_map 再次从服务端确认，或者直接使用内存中的 map
        # 这里直接使用内存累积的 map，它是最准确的（包含本次上传结果）
        if not hasattr(self, '_final_parts_map') or not self._final_parts_map:
            # 兜底：如果内存没数据（比如流是空的），尝试查服务端
            self._final_parts_map = self._fetch_uploaded_parts_map(key, uid)

        if not self._final_parts_map:
            raise Exception("未找到任何分片，无法合并文件")

        # 构造合并请求列表 (必须按 PartNum 排序)
        sorted_parts = [
            CompletePart(partNum=k, etag=v)
            for k, v in sorted(self._final_parts_map.items())
        ]

        resp = self.client.completeMultipartUpload(
            self.bucket, key, uid,
            CompleteMultipartUploadRequest(sorted_parts)
        )
        self._check_error(resp)
        logger.info(f"✅ 上传成功: {key}")

    def _check_error(self, resp):
        if resp.status >= 300:
            raise Exception(f"OBS Error {resp.errorCode}: {resp.errorMessage}")