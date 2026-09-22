"""高优先级修复：max_workers、total_size 语义、进度条、日志、猴子补丁、异常日志。"""
import logging
import math
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from pyobs.core import StreamUploader, UploadContext, apply_obs_ssl_patch
from pyobs.exceptions import PartLimitExceededError


def _make_uploader(**kwargs):
    with patch('pyobs.core.ObsClient') as MockObsClient:
        mock_client = MockObsClient.return_value
        uploader = StreamUploader(
            ak="test", sk="test", server="test", bucket_name="test", **kwargs
        )
        uploader._complete_upload = MagicMock()
        return uploader, mock_client


class TestMaxWorkers(unittest.TestCase):
    def test_thread_pool_uses_configured_workers(self):
        uploader, _ = _make_uploader(max_workers=3, part_size=10)
        uploader._fetch_uploaded_parts_map = MagicMock(return_value=({}, 0))
        uploader._upload_part_with_retry = MagicMock(return_value="etag")

        with patch('pyobs.core.ThreadPoolExecutor') as mock_pool:
            mock_executor = MagicMock()
            mock_pool.return_value.__enter__.return_value = mock_executor
            mock_future = MagicMock()
            mock_future.result.return_value = "etag"
            mock_executor.submit.return_value = mock_future

            # as_completed 需要可迭代 futures
            with patch('pyobs.core.as_completed', return_value=[]):
                uploader._process_stream(iter([b"x" * 5]), "key", "uid", 1, 5, 10)

            mock_pool.assert_called_with(max_workers=3)


class TestTotalSizeSemantics(unittest.TestCase):
    """total_size 必须始终是完整对象大小，不是剩余大小。"""

    def test_resume_subtracts_offset_from_complete_size(self):
        uploader, _ = _make_uploader(part_size=20 * 1024 * 1024)
        uploader._process_stream = MagicMock(return_value=0)

        offset = 100 * 1024 ** 3
        total_size = 500 * 1024 ** 3
        next_part = 5000
        context = UploadContext("key", "uid", offset, next_part)

        uploader.upload_stream(context, iter([]), total_size=total_size)

        remaining_size = total_size - offset
        remaining_parts = StreamUploader.MAX_PARTS - next_part + 1
        safe_parts = max(int(remaining_parts * 0.8), 100)
        expected = math.ceil(remaining_size / safe_parts)

        call_args = uploader._process_stream.call_args
        self.assertEqual(call_args[0][5], expected)
        self.assertEqual(call_args.kwargs.get("uploaded_offset", call_args[1].get("uploaded_offset")), offset)

    def test_rejects_total_size_smaller_than_offset(self):
        """误把 Range Content-Length 当成 total_size 时应立刻报错。"""
        uploader, _ = _make_uploader()
        context = UploadContext("key", "uid", offset=1000, next_part=2)
        with self.assertRaises(ValueError) as cm:
            uploader.upload_stream(context, iter([]), total_size=200)
        self.assertIn("完整对象大小", str(cm.exception))

    def test_new_upload_treats_total_size_as_complete(self):
        uploader, _ = _make_uploader(part_size=20 * 1024 * 1024)
        uploader._process_stream = MagicMock(return_value=0)
        context = UploadContext("key", "uid", 0, 1)
        total_size = 500 * 1024 ** 3
        uploader.upload_stream(context, iter([]), total_size=total_size)
        expected = math.ceil(total_size / StreamUploader.SAFE_PARTS_COUNT)
        self.assertEqual(uploader._process_stream.call_args[0][5], expected)


class TestProgressBarOffset(unittest.TestCase):
    def test_progress_uses_real_offset_not_part_estimate(self):
        uploader, _ = _make_uploader(part_size=10)
        uploader._fetch_uploaded_parts_map = MagicMock(return_value=({}, 0))
        uploader._upload_part_with_retry = MagicMock(return_value="etag")

        real_offset = 12345  # 故意与 (start_part-1)*part_size 不同
        start_part = 6
        # 旧算法会得到 (6-1)*10 = 50，新算法必须用 12345

        with patch('pyobs.core.tqdm') as mock_tqdm:
            mock_pbar = MagicMock()
            mock_tqdm.return_value = mock_pbar
            uploader._process_stream(
                iter([b"abc"]),
                "dir/file.bin",
                "uid",
                start_part,
                20000,
                10,
                uploaded_offset=real_offset,
            )
            kwargs = mock_tqdm.call_args.kwargs
            self.assertEqual(kwargs["initial"], real_offset)
            self.assertEqual(kwargs["total"], 20000)
            self.assertNotEqual(kwargs["initial"], (start_part - 1) * 10)

    def test_progress_falls_back_to_listed_part_bytes(self):
        """未传入 offset 时，用 listParts 的真实 size 之和，而不是分片数估算。"""
        uploader, _ = _make_uploader(part_size=10)
        uploader._fetch_uploaded_parts_map = MagicMock(return_value=({1: "a", 2: "b"}, 777))
        uploader._upload_part_with_retry = MagicMock(return_value="etag")

        with patch('pyobs.core.tqdm') as mock_tqdm:
            mock_tqdm.return_value = MagicMock()
            uploader._process_stream(
                iter([b"x"]), "key", "uid", 3, 1000, 10, uploaded_offset=0
            )
            self.assertEqual(mock_tqdm.call_args.kwargs["initial"], 777)


class TestSuccessLogLevel(unittest.TestCase):
    def test_part_success_is_debug_not_info(self):
        uploader, mock_client = _make_uploader()
        mock_client.uploadPart.return_value.status = 200
        mock_client.uploadPart.return_value.body.etag = "etag"

        logger = logging.getLogger("ObsStream")
        with self.assertLogs(logger, level="DEBUG") as cm:
            # 强制把 logger 放到 DEBUG，assertLogs 会临时提升
            result = uploader._upload_part_with_retry("key", "uid", 1, b"data")
        self.assertEqual(result, "etag")
        success_logs = [r for r in cm.records if "上传成功" in r.getMessage()]
        self.assertTrue(success_logs)
        self.assertTrue(all(r.levelno == logging.DEBUG for r in success_logs))


class TestMonkeyPatch(unittest.TestCase):
    def test_patch_skipped_when_disabled(self):
        with patch('pyobs.core.ObsClient'):
            with patch('pyobs.core.apply_obs_ssl_patch') as mock_patch:
                StreamUploader(
                    ak="t", sk="t", server="t", bucket_name="t", patch_obs_ssl=False
                )
                mock_patch.assert_not_called()

                StreamUploader(
                    ak="t", sk="t", server="t", bucket_name="t", patch_obs_ssl=True
                )
                mock_patch.assert_called()

    def test_apply_obs_ssl_patch_is_idempotent(self):
        # 无论之前是否已经打过，重复调用不应抛异常
        apply_obs_ssl_patch()
        apply_obs_ssl_patch()
        from pyobs import core as core_mod
        self.assertTrue(core_mod._OBS_SSL_PATCHED)


class TestAbortWarning(unittest.TestCase):
    def test_wb_mode_logs_warning_when_abort_fails(self):
        uploader, mock_client = _make_uploader()
        uploader._process_stream = MagicMock(return_value=0)
        mock_client.abortMultipartUpload.side_effect = RuntimeError("network down")
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.body.uploadId = "new_id"
        mock_client.initiateMultipartUpload.return_value = mock_resp

        context = UploadContext("key", "old_id", 50, 6)
        logger = logging.getLogger("ObsStream")
        with self.assertLogs(logger, level="WARNING") as cm:
            uploader.upload_stream(context, iter([]), mode="wb")
        self.assertTrue(any("abortMultipartUpload" in r.getMessage() for r in cm.records))
        self.assertEqual(context.upload_id, "new_id")
        self.assertEqual(context.offset, 0)

    def test_empty_stream_abort_failure_logs_warning(self):
        uploader, mock_client = _make_uploader()
        # _make_uploader 把 _complete_upload mock 掉了，这里测的是真实实现
        uploader._complete_upload = StreamUploader._complete_upload.__get__(uploader, StreamUploader)
        uploader._final_parts_map = {}
        mock_client.listParts.return_value.status = 200
        mock_client.listParts.return_value.body.parts = []
        mock_client.listParts.return_value.body.isTruncated = False
        mock_client.abortMultipartUpload.side_effect = RuntimeError("boom")
        logger = logging.getLogger("ObsStream")
        with self.assertLogs(logger, level="WARNING") as cm:
            StreamUploader._complete_upload(uploader, "key", "uid")
        self.assertTrue(any("取消空任务失败" in r.getMessage() for r in cm.records))


class TestPartLimitErrorHandling(unittest.TestCase):
    def test_part_limit_exceeded_on_resume(self):
        uploader, _ = _make_uploader(part_size=5)
        # 已用掉几乎全部分片，剩余数据远超剩余分片容量
        context = UploadContext("key", "uid", offset=50, next_part=9999)
        with self.assertRaises(PartLimitExceededError):
            uploader.upload_stream(
                context,
                iter([]),
                total_size=50 + 10_000_000,
                mode="ab",
            )


if __name__ == '__main__':
    unittest.main()
