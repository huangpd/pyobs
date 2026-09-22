from __future__ import annotations

import os
import math
import time
import logging
from collections.abc import Iterable
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from obs import ObsClient, CompleteMultipartUploadRequest, CompletePart, ListMultipartUploadsRequest
from tqdm import tqdm
import http.client
from .exceptions import PartLimitExceededError, PartUploadError

logger = logging.getLogger("ObsStream")

# 仅在显式调用 apply_obs_ssl_patch() / StreamUploader(patch_obs_ssl=True) 时打补丁，
# 避免 import 时改写全局 ObsClient。
_OBS_SSL_PATCHED = False


def _patched_get_server_connection(self, is_secure, server, port, proxy_host, proxy_port):
    """覆盖 SDK 原有的连接创建方法，移除不支持的 check_hostname 参数"""
    if proxy_host is not None and proxy_port is not None:
        server = proxy_host
        port = proxy_port

    if is_secure:
        try:
            conn = http.client.HTTPSConnection(
                server,
                port=port,
                timeout=self.timeout,
                context=self.context
            )
        except TypeError:
            conn = http.client.HTTPSConnection(
                server,
                port=port,
                timeout=self.timeout
            )
    else:
        conn = http.client.HTTPConnection(server, port=port, timeout=self.timeout)

    return conn


def apply_obs_ssl_patch() -> bool:
    """给 huaweicloud-sdk-python-obs 打 SSL 连接补丁。幂等，可安全重复调用。

    Returns:
        True 表示本次实际打上了补丁，False 表示此前已经打过。
    """
    global _OBS_SSL_PATCHED
    if _OBS_SSL_PATCHED:
        return False
    # 始终补丁真实的 obs.ObsClient，即使调用方把 pyobs.core.ObsClient mock 掉了
    from obs import ObsClient as _RealObsClient
    _RealObsClient._get_server_connection_use_http1x = _patched_get_server_connection
    _OBS_SSL_PATCHED = True
    logger.debug("已为 ObsClient 应用 SSL 连接补丁")
    return True


class UploadContext:
    """
    上传上下文：用于在初始化和实际上传之间传递状态
    """

    def __init__(self, object_key: str, upload_id: str, offset: int, next_part: int) -> None:
        self.key = object_key
        self.upload_id = upload_id  # OBS 内部任务 ID (用于续传)
        self.offset = offset  # 已上传字节数 (用于告诉下载器 Range)
        self.next_part = next_part  # 下一个分片号 (用于分片排序)


class StreamUploader:
    """
    华为云 OBS 流式上传工具 (支持断点续传)
    """

    # --- 分片上传常量 ---
    MAX_PARTS = 10000  # OBS 分片上传的硬性上限
    SAFE_PARTS_COUNT = 8000  # 安全分片数，预留 20% 缓冲应对 Content-Length 不准确
    SAFETY_THRESHOLD = 5000  # 调整为 5000，提早介入，避免后期内存激增
    MIN_PART_SIZE = 5 * 1024 * 1024  # 5MB (S3/OBS 协议要求前 N-1 个分片 >= 5MB)
    DEFAULT_MAX_WORKERS = 5
    DEFAULT_STREAM_PART_SIZE = 150 * 1024 * 1024  # 未知总大小时的默认分片，约支持 1.2TB / 8000 parts

    def __init__(
        self,
        ak: str | None = None,
        sk: str | None = None,
        server: str | None = None,
        bucket_name: str | None = None,
        part_size: int = 20 * 1024 * 1024,
        max_workers: int = DEFAULT_MAX_WORKERS,
        patch_obs_ssl: bool = True,
    ) -> None:
        # 优先从环境变量读取配置，支持无参初始化
        self.ak = ak or os.getenv("OBS_AK")
        self.sk = sk or os.getenv("OBS_SK")
        self.server = server or os.getenv("OBS_SERVER")
        self.bucket = bucket_name or os.getenv("OBS_BUCKET")

        if not all([self.ak, self.sk, self.server, self.bucket]):
            raise ValueError("必须提供 AK, SK, Server 和 Bucket Name (可通过参数或环境变量)")

        if max_workers < 1:
            raise ValueError("max_workers 必须 >= 1")
        if part_size < 1:
            raise ValueError("part_size 必须 >= 1")

        if patch_obs_ssl:
            apply_obs_ssl_patch()

        self.client = ObsClient(access_key_id=self.ak, secret_access_key=self.sk, server=self.server)

        # --- 核心配置 ---
        # 分片大小默认 20MB，既能保证并发度，又适配流式场景
        self.part_size = part_size
        self.max_workers = max_workers
        self.max_retries = 5  # 单个分片上传失败重试次数

    def abort_upload(self, object_key: str, upload_id: str | None = None) -> bool:
        """
        取消指定的上传任务（清理已上传的分片）
        :param object_key: OBS 目标路径
        :param upload_id: (可选) 指定的 upload_id，如果不传则自动查找
        :return: True 成功取消，False 未找到任务
        """
        if not upload_id:
            # 自动查找该 key 的上传任务
            upload_id, _, next_part = self._get_resume_info(object_key)
            if not upload_id:
                logger.info(f"未找到 {object_key} 的未完成上传任务")
                return False
            logger.info(f"找到任务: {upload_id}, 已有分片: {next_part - 1}")

        try:
            resp = self.client.abortMultipartUpload(self.bucket, object_key, upload_id)
            if resp.status < 300:
                logger.info(f"✅ 已成功取消上传任务: {object_key} (ID: {upload_id})")
                return True
            else:
                logger.error(f"取消失败: {resp.errorMessage}")
                return False
        except Exception as e:
            logger.error(f"取消任务时发生错误: {e}")
            return False

    def init_upload(self, object_key: str) -> UploadContext:
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

    def upload_stream(
        self,
        context: UploadContext,
        stream_iterator: Iterable[bytes],
        total_size: int | None = None,
        mode: str = "ab",
    ) -> int:
        """
        【第二步】接收数据流，执行并发分片上传

        :param context: init_upload 返回的 UploadContext 对象
        :param stream_iterator: 数据流 (bytes 生成器)
        :param total_size: (可选) **完整对象/文件的总大小**（字节），不是剩余大小。
            - 用于分片大小计算、进度条、上传前校验。
            - 断点续传时请传「文件总字节数」，不要传 Range 响应的 Content-Length。
            - HTTP 206 时：``total_size = context.offset + int(resp.headers["Content-Length"])``
            - HTTP 200 时：``total_size = int(resp.headers["Content-Length"])``
        :param mode: "ab" 代表追加(续传)，"wb" 代表覆盖(重传)
        :return: 文件在 OBS 上的总大小 (int) -> (旧offset + 本次上传量)
        """
        if mode == "wb":
            logger.info(f"模式为 wb，正在清理并重置任务: {context.key}")
            try:
                self.client.abortMultipartUpload(self.bucket, context.key, context.upload_id)
            except Exception as e:
                logger.warning(f"abortMultipartUpload 失败（将继续创建新任务）: {context.key}: {e}")

            resp = self.client.initiateMultipartUpload(self.bucket, context.key)
            self._check_error(resp)
            context.upload_id = resp.body.uploadId
            context.offset = 0
            context.next_part = 1

        logger.info(f"开始接收数据流，写入: {context.key} (Mode: {mode}, Offset: {context.offset})")

        current_part_size = self.part_size
        remaining_parts = self.MAX_PARTS - context.next_part + 1

        # total_size 语义：始终是完整对象大小。剩余量 = total_size - 已上传字节。
        if total_size and total_size > 0:
            if total_size < context.offset:
                raise ValueError(
                    f"total_size ({total_size}) 必须是完整对象大小，不能小于已上传字节 "
                    f"context.offset ({context.offset})。"
                    f"Range 请求请使用: total_size = context.offset + Content-Length"
                )
            remaining_size = total_size - context.offset

            if context.offset == 0:
                safe_parts_count = self.SAFE_PARTS_COUNT  # 新任务用 8000
            else:
                safe_parts_count = int(remaining_parts * 0.8)
                safe_parts_count = max(safe_parts_count, 100)  # 至少保留 100 个分片

            min_part_size = math.ceil(remaining_size / safe_parts_count) if remaining_size > 0 else current_part_size

            if min_part_size > current_part_size:
                if context.offset > 0:
                    logger.warning(f"[断点续传] 剩余数据 ({remaining_size / 1024 / 1024 / 1024:.2f} GB)，"
                                   f"剩余分片数 {remaining_parts}，"
                                   f"调整分片大小: {current_part_size / 1024 / 1024:.0f}MB -> {min_part_size / 1024 / 1024:.0f}MB")
                else:
                    logger.warning(f"文件大小 ({total_size / 1024 / 1024 / 1024:.2f} GB) 较大，"
                                   f"自动调整分片大小: {current_part_size / 1024 / 1024:.0f}MB -> {min_part_size / 1024 / 1024:.0f}MB "
                                   f"(目标分片数 ~{safe_parts_count})")
                current_part_size = min_part_size

            estimated_parts_needed = math.ceil(remaining_size / current_part_size) if remaining_size > 0 else 0
            if estimated_parts_needed > remaining_parts:
                raise PartLimitExceededError(
                    f"分片数不足！剩余分片数: {remaining_parts}，"
                    f"预计需要: {estimated_parts_needed}，"
                    f"剩余数据: {remaining_size / 1024 / 1024 / 1024:.2f} GB，"
                    f"当前分片大小: {current_part_size / 1024 / 1024:.0f} MB。"
                    f"建议：放弃当前任务 (mode='wb') 并使用更大的分片大小重新上传。",
                    remaining_parts=remaining_parts,
                    estimated_parts=estimated_parts_needed,
                    remaining_size=remaining_size,
                    part_size=current_part_size
                )

        else:
            if current_part_size < self.DEFAULT_STREAM_PART_SIZE:
                logger.info(
                    f"未知总大小，自动将分片大小从 {current_part_size / 1024 / 1024:.0f}MB "
                    f"提升至 {self.DEFAULT_STREAM_PART_SIZE / 1024 / 1024:.0f}MB 以支持大文件"
                )
                current_part_size = self.DEFAULT_STREAM_PART_SIZE

        try:
            bytes_uploaded = self._process_stream(
                stream_iterator,
                context.key,
                context.upload_id,
                context.next_part,
                total_size,
                current_part_size,
                uploaded_offset=context.offset,
            )

            self._complete_upload(context.key, context.upload_id)
            return context.offset + bytes_uploaded

        except Exception as e:
            logger.error(f"上传过程中断: {e}")
            raise e

    def _get_resume_info(self, key: str) -> tuple[str | None, int, int]:
        """查询 OBS 服务端是否存在未完成的分段任务"""
        list_req = ListMultipartUploadsRequest(prefix=key)
        resp = self.client.listMultipartUploads(self.bucket, multipart=list_req)

        target_id = None
        if resp.status < 300 and resp.body.upload:
            for upload in resp.body.upload:
                if upload.key == key:
                    target_id = upload.uploadId
                    break

        if not target_id:
            return None, 0, 1

        uploaded_bytes = 0
        next_part = 1
        marker = None

        while True:
            parts_resp = self.client.listParts(self.bucket, key, target_id, partNumberMarker=marker)
            if parts_resp.status >= 300:
                logger.warning(f"查询分片失败: {parts_resp.errorMessage}")
                return None, 0, 1

            for part in parts_resp.body.parts:
                if part.partNumber == next_part:
                    uploaded_bytes += part.size
                    next_part += 1

            if not parts_resp.body.isTruncated:
                break
            marker = parts_resp.body.nextPartNumberMarker

        return target_id, uploaded_bytes, next_part

    def _process_stream(
        self,
        iterator,
        key: str,
        uid: str,
        start_part: int,
        total_size: int | None,
        part_size: int,
        uploaded_offset: int = 0,
    ) -> int:
        """读取流 -> 缓冲 -> 提交线程池 (支持动态分片大小调整)

        :param uploaded_offset: 续传前已经在 OBS 上的真实字节数（context.offset），
            用于进度条 initial 和动态分片计算，禁止用 (start_part-1)*part_size 估算。
        """
        buffer = BytesIO()
        part_number = start_part
        total_stream_bytes = 0
        current_part_size = part_size

        parts_map: dict[int, str] = {}
        if start_part > 1:
            parts_map, listed_bytes = self._fetch_uploaded_parts_map(key, uid)
            if uploaded_offset <= 0 and listed_bytes > 0:
                uploaded_offset = listed_bytes

        pbar = None
        if tqdm:
            pbar = tqdm(
                total=total_size,
                initial=uploaded_offset,
                unit='B',
                unit_scale=True,
                desc=f"🚀 Uploading {os.path.basename(key)}",
                mininterval=1,
                position=0,
                dynamic_ncols=True
            )

        last_warned_part = [-1]

        def calculate_dynamic_part_size(current_part, current_size, uploaded_bytes, total_file_size):
            """计算动态分片大小（在每个分片上传前调用）
            uploaded_bytes 必须是「已在对象上的真实字节数」（历史 offset + 本次已读）。
            """
            if current_part >= self.SAFETY_THRESHOLD:
                if total_file_size:
                    remaining_parts = self.MAX_PARTS - current_part
                    estimated_remaining = total_file_size - uploaded_bytes

                    if remaining_parts > 0 and estimated_remaining > 0:
                        new_size = estimated_remaining // remaining_parts
                        if new_size > current_size:
                            adjusted_size = max(new_size, self.MIN_PART_SIZE)

                            if last_warned_part[0] != current_part:
                                logger.warning(f"分片 {current_part} 接近阈值 {self.SAFETY_THRESHOLD} (From TotalSize)，动态调整分片大小为 {adjusted_size} bytes")
                                last_warned_part[0] = current_part

                            return adjusted_size

                else:
                    ratio = (current_part - self.SAFETY_THRESHOLD) / (self.MAX_PARTS - self.SAFETY_THRESHOLD)

                    multiplier = 1
                    if ratio > 0.8:  # > 9000
                        multiplier = 10
                    elif ratio > 0.6:  # > 8000
                        multiplier = 5
                    elif ratio > 0.4:  # > 7000
                        multiplier = 2
                    elif ratio >= 0:  # 5000 ~ 7000
                        multiplier = 1.5

                    new_size = int(current_size * multiplier)
                    if new_size > current_size:
                        if last_warned_part[0] != current_part:
                            logger.warning(f"分片 {current_part} 接近阈值 {self.SAFETY_THRESHOLD} (Unknown TotalSize)，平滑扩容: {current_size / 1024 / 1024:.0f}MB -> {new_size / 1024 / 1024:.0f}MB")
                            last_warned_part[0] = current_part
                        return new_size

            return current_size

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {}

            try:
                for chunk in iterator:
                    if chunk:
                        total_stream_bytes += len(chunk)
                        buffer.write(chunk)

                        if pbar is not None:
                            pbar.update(len(chunk))

                        bytes_on_object = uploaded_offset + total_stream_bytes
                        effective_part_size = calculate_dynamic_part_size(
                            part_number, current_part_size, bytes_on_object, total_size
                        )

                        if buffer.tell() >= effective_part_size:
                            buffer.seek(0)

                            while True:
                                remaining_len = buffer.getbuffer().nbytes - buffer.tell()

                                if remaining_len < effective_part_size:
                                    break

                                data = buffer.read(effective_part_size)

                                f = executor.submit(self._upload_part_with_retry, key, uid, part_number, data)
                                futures[f] = part_number

                                part_number += 1

                                if len(futures) >= self.max_workers * 2:
                                    self._wait_and_collect(futures, parts_map)

                                effective_part_size = calculate_dynamic_part_size(
                                    part_number, current_part_size, bytes_on_object, total_size
                                )

                            remaining_data = buffer.read()
                            buffer = BytesIO()
                            buffer.write(remaining_data)

                if buffer.tell() > 0:
                    f = executor.submit(self._upload_part_with_retry, key, uid, part_number, buffer.getvalue())
                    futures[f] = part_number

                for f in as_completed(futures):
                    p_num = futures[f]
                    etag = f.result()
                    parts_map[p_num] = etag

            except Exception as e:
                raise e
            finally:
                if pbar is not None:
                    pbar.close()

        self._final_parts_map = parts_map
        return total_stream_bytes

    def _upload_part_with_retry(self, key: str, uid: str, p_num: int, data: bytes):
        """带重试机制的单个分片上传。成功日志走 DEBUG，避免几百个分片刷屏。"""
        if p_num > self.MAX_PARTS:
            raise PartLimitExceededError(
                f"分片号 {p_num} 超过上限 {self.MAX_PARTS}，无法继续上传",
                remaining_parts=0,
                estimated_parts=p_num
            )

        data_len = len(data)

        for i in range(self.max_retries):
            try:
                start_time = time.time()
                resp = self.client.uploadPart(
                    bucketName=self.bucket, objectKey=key, partNumber=p_num,
                    uploadId=uid, content=data, partSize=data_len
                )
                if resp.status < 300:
                    duration = time.time() - start_time
                    speed = (data_len / 1024 / 1024) / duration if duration > 0 else 0
                    logger.debug(
                        f"分片 #{p_num} 上传成功 | "
                        f"大小: {data_len / 1024 / 1024:.2f}MB | "
                        f"耗时: {duration:.1f}s | "
                        f"速度: {speed:.1f}MB/s"
                    )
                    return resp.body.etag
                else:
                    logger.warning(
                        f"⚠️ 分片 #{p_num} 上传失败 (HTTP {resp.status}, Code: {resp.errorCode}, Msg: {resp.errorMessage})，正在重试 {i + 1}/{self.max_retries}...")

            except Exception as ex:
                logger.warning(f"❌ 分片 #{p_num} 发生异常: {ex}，正在重试 {i + 1}/{self.max_retries}...")
            time.sleep(1 * (i + 1))
        raise PartUploadError(f"分片 #{p_num} 在 {self.max_retries} 次尝试后最终失败", part_number=p_num)

    def _wait_and_collect(self, futures, parts_map):
        """等待部分任务完成，回收内存，收集 ETag"""
        done, _ = wait(futures.keys(), return_when=FIRST_COMPLETED)
        for f in done:
            p_num = futures.pop(f)
            try:
                parts_map[p_num] = f.result()
            except Exception as e:
                raise e

    def _fetch_uploaded_parts_map(self, key: str, uid: str) -> tuple[dict[int, str], int]:
        """获取服务端已有的分片信息 (PartNum -> ETag) 以及已上传字节总和。"""
        mapping: dict[int, str] = {}
        total_bytes = 0
        marker = None
        while True:
            resp = self.client.listParts(self.bucket, key, uid, partNumberMarker=marker)
            if resp.status >= 300:
                logger.warning(f"listParts 失败: {getattr(resp, 'errorMessage', resp.status)}")
                break
            for p in resp.body.parts:
                mapping[p.partNumber] = p.etag
                size = getattr(p, "size", 0) or 0
                total_bytes += size
            if not resp.body.isTruncated:
                break
            marker = resp.body.nextPartNumberMarker
        return mapping, total_bytes

    def _complete_upload(self, key: str, uid: str) -> None:
        """合并分片"""
        logger.info("流传输结束，正在请求合并分片...")

        if not hasattr(self, '_final_parts_map') or not self._final_parts_map:
            self._final_parts_map, _ = self._fetch_uploaded_parts_map(key, uid)

        if not self._final_parts_map:
            logger.warning(f"⚠️ 数据流为空（0 字节），跳过上传: {key}")
            try:
                self.client.abortMultipartUpload(self.bucket, key, uid)
                logger.info(f"已取消空任务: {key}")
            except Exception as e:
                logger.warning(f"取消空任务失败: {key}: {e}")
            return

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

    def _check_error(self, resp) -> None:
        if resp.status >= 300:
            raise Exception(f"OBS Error {resp.errorCode}: {resp.errorMessage}")
