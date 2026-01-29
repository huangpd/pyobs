import httpx
from dotenv import load_dotenv
import os
from tqdm import tqdm
from crawler import RabbitMQScheduler, Request, Result, Crawler, Response
from crawler.core.enums import NodeType
from obs_file import open as obs_open, getsize
from crawler.utils.logger import get_logger
from pyobs import StreamUploader, PartLimitExceededError, PartUploadError
from crawler._http import HttpxDownloader, CurlCffiDownloader
logger = get_logger()
load_dotenv()


def format_file_size(size_bytes: int) -> str:
    """将字节数格式化为人类可读的格式"""
    if size_bytes == 0:
        return "0 B"

    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1

    return f"{size_bytes:.2f} {size_names[i]}"


@RabbitMQScheduler.crawl
#@CurlCffiDownloader.use
class EmblCraw(Crawler):

    bucket = "sais-craw-abroad"
    block_size = 10 * 1024 * 1024  # 10MB

    uploader = StreamUploader(
        ak="OEIXE6THI9OSTPBNUTY8",
        sk="lNHJhr306Qt5iHS73F3KZqOdT2mTRQdI2qMMMHOX",
        server="obs.ap-southeast-3.myhuaweicloud.com",
        bucket_name="sais-craw-abroad"
    )
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
    }
    cookies = {}

    with open('urls.txt', 'r', encoding='utf-8') as f:
        _pairs = [
            line.strip().split(maxsplit=1)
            for line in f
            if line.strip()
        ]

    start_request = [
        Request(
            node_type=NodeType.LEAF,
            url=url,
            callback="download_file",
            resource_path=f"{path}",
            stream=True,
            meta={},
        )
        for url, path in _pairs
    ]

    def process_request(self, request: 'Request') -> 'Request':
        """
        自动判断本地是否有已下载部分 → 设置 Range 头
        如果文件不存在或不是文件 → 返回 request
        """
        # 确保 headers 是字典
        if request.headers is None:
            request.headers = {}

        if request.resource_path:
            save_path = request.resource_path.replace('/obs/', '')
        else:
            raise 'Not resource_path'

        try:
            context = self.uploader.init_upload(object_key=save_path)
            request.meta['context'] = context
            start_size = context.offset
            logger.info(f"已上传文件大小: {format_file_size(start_size)}")
        except FileNotFoundError:
            start_size = 0
            logger.info("未发现已上传文件")

        if start_size > 0:
            # 已有部分 —> 尝试加 Range
            request.headers['Range'] = f"bytes={start_size}-"
            request.stream = True
            request.headers['Accept-Encoding'] = 'identity'
            logger.info(f"设置 Range: bytes={start_size}-")

        return request

    def download_file(self, response: 'Response') -> Result|None:
        """
        自动执行 OBS 下载逻辑：
        - 只处理 type=file 的请求
        - 支持断点续传（ab）或完整下载（wb）
        - 已完整下载直接跳过
        """
        file_url = str(response.url)
        if not response.resource_request.resource_path:
            logger.error(f"Download failed: resource_path is missing for {file_url}")
            return None

        save_path = response.resource_request.resource_path.replace('/obs/', '')
        context = response.resource_request.meta.get('context')
        try:
            logger.info(f"OBS 已有: {format_file_size(context.offset)}")
            # 获取远程文件总大小
            total_size = self.get_remote_file_size(file_url)
            if total_size == 0:
                logger.info(f"无法获取远程文件大小，使用原 response.content 完整下载")
            # 文件已完整
            if context.offset >= total_size > 0:
                logger.info(f"文件已完整，跳过下载: {save_path}")
                return Result(resource_path=f"{save_path}", resource_size=context.offset)

            logger.info(f"开始下载文件: {save_path}")
            logger.info(f"HTTP 状态码: {response.status_code}")
            logger.info("Response headers:", response.headers)

            # 判断下载模式
            status = response.status_code
            if status == 206:
                # 检查 Content-Range 是否真的是从断点开始
                content_range = response.headers.get('Content-Range', '')
                if content_range:
                    import re
                    match = re.match(r'bytes (\d+)-(\d+)/(\d+)', content_range)
                    if match:
                        start_byte = int(match.group(1))
                        if start_byte == context.offset:
                            logger.info(f"真正的断点续传: 从 {start_byte} 开始")
                            mode = "ab"
                        else:
                            logger.warning(
                                f"Content-Range 起始位置 {start_byte} != 本地偏移 {context.offset}, 使用完整下载")
                            mode = "wb"
                    else:
                        logger.warning("无法解析 Content-Range, 使用完整下载")
                        mode = "wb"
                else:
                    logger.warning("206 但无 Content-Range, 使用完整下载")
                    mode = "wb"

            elif status == 200:
                # 检查 Content-Length 是否等于剩余部分
                content_length = int(response.headers.get('Content-Length', 0))
                expected_remaining = total_size - context.offset if total_size > 0 else 0
                if expected_remaining > 0 and abs(content_length - expected_remaining) < 1024:
                    # 可能支持 Range 但返回 200 (某些 CDN)
                    logger.info(f"200 但 Content-Length 匹配剩余部分, 尝试续传")
                    mode = "ab"
                else:
                    logger.info("200 不支持 Range 或已有数据无效, 重新下载")
                    mode = "wb"

            elif status == 416:
                logger.info("416 文件已完整")
                return Result(resource_path=save_path, resource_size=context.offset)
            else:
                logger.warning(f"未预期的状态码 {status}, 使用完整下载")
                mode = "wb"
            try:
                downloaded_bytes = self.uploader.upload_stream(
                    context=context,
                    stream_iterator=response.iter_bytes(chunk_size=20 * 1024 * 1024),
                    mode=mode,
                    total_size=total_size,
                )
                logger.info(f'数据下载完成{save_path}')
                return Result(resource_path=f"{save_path}", resource_size=downloaded_bytes)
            except PartLimitExceededError as e:
                # 分片数不足，需要清理并重新上传
                logger.warning(f"分片数不足: 剩余{e.remaining_parts}, 需要{e.estimated_parts}")
                self.uploader.abort_upload(save_path)
                raise e
            except PartUploadError as e:
                # 单个分片上传失败（重试后仍失败）
                logger.error(f"分片 #{e.part_number} 上传失败")
                raise e
            except Exception as e:
                logger.info(f"OBS 其他异常: {e}")
                raise e
        finally:
            response.close()

    def get_remote_file_size(self, url: str, timeout=30) -> int:
        """
        使用 httpx HEAD 请求获取远程文件总大小
        返回文件大小（字节），获取失败返回 0
        """
        try:
            with httpx.Client(timeout=timeout) as client:
                resp = client.head(url, headers=self.headers, cookies=self.cookies, follow_redirects=True)
                resp.raise_for_status()
                size = int(resp.headers.get("Content-Length", 0))
                return size
        except Exception as e:
            logger.info(f"获取远程文件大小失败: {url}, {e}")
            return 0


if __name__ == '__main__':
    crawler_instance = EmblCraw()
    #crawler_instance.init(push_request=False)
    crawler_instance.run_for_ever()