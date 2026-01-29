import sys
import os
import unittest
from io import BytesIO
from unittest.mock import MagicMock, patch

# Add src to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from pyobs.core import StreamUploader, UploadContext


class TestStreamUploaderConstants(unittest.TestCase):
    """测试类常量定义"""

    def test_max_parts_constant(self):
        """测试 MAX_PARTS 常量存在且值为 10000"""
        self.assertEqual(StreamUploader.MAX_PARTS, 10000)

    def test_safety_threshold_constant(self):
        """测试 SAFETY_THRESHOLD 常量存在且值为 5000"""
        self.assertEqual(StreamUploader.SAFETY_THRESHOLD, 5000)

    def test_safe_parts_count_constant(self):
        """测试 SAFE_PARTS_COUNT 常量存在且值为 8000"""
        self.assertEqual(StreamUploader.SAFE_PARTS_COUNT, 8000)

    def test_min_part_size_constant(self):
        """测试 MIN_PART_SIZE 常量存在且值为 5MB"""
        self.assertEqual(StreamUploader.MIN_PART_SIZE, 5 * 1024 * 1024)


class TestStreamUploaderInit(unittest.TestCase):
    """测试 __init__ 方法"""

    def test_init_default_part_size(self):
        """测试默认分片大小为 20MB"""
        with patch('pyobs.core.ObsClient') as MockObsClient:
            uploader = StreamUploader(
                ak="test_ak", sk="test_sk", server="test_server", bucket_name="test_bucket"
            )
            self.assertEqual(uploader.part_size, 20 * 1024 * 1024)


class TestStreamUploader(unittest.TestCase):
    def setUp(self):
        # Create a dummy StreamUploader with mocked ObsClient
        with patch('pyobs.core.ObsClient') as MockObsClient:
            self.mock_client = MockObsClient.return_value
            self.uploader = StreamUploader(
                ak="test_ak", sk="test_sk", server="test_server", bucket_name="test_bucket"
            )
            # Reduce part size for easier testing
            self.uploader.part_size = 10  # 10 bytes per part
            self.uploader.max_workers = 2

    def test_init_upload_new_task(self):
        """Test initializing a new upload task (no resume)."""
        # Mock _get_resume_info to return nothing
        self.uploader._get_resume_info = MagicMock(return_value=(None, 0, 1))
        
        # Mock initiateMultipartUpload response
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.body.uploadId = "new_upload_id"
        self.mock_client.initiateMultipartUpload.return_value = mock_resp

        context = self.uploader.init_upload("test_key")

        self.assertEqual(context.key, "test_key")
        self.assertEqual(context.upload_id, "new_upload_id")
        self.assertEqual(context.offset, 0)
        self.assertEqual(context.next_part, 1)

    def test_init_upload_resume_task(self):
        """Test initializing a resume upload task."""
        # Mock _get_resume_info to return existing task info
        self.uploader._get_resume_info = MagicMock(return_value=("existing_id", 100, 3))

        context = self.uploader.init_upload("test_key")

        self.assertEqual(context.upload_id, "existing_id")
        self.assertEqual(context.offset, 100)
        self.assertEqual(context.next_part, 3)
        # Should NOT call initiateMultipartUpload
        self.mock_client.initiateMultipartUpload.assert_not_called()

    def test_upload_stream_ab_mode(self):
        """Test upload_stream in default 'ab' (append) mode."""
        context = UploadContext("test_key", "test_id", offset=20, next_part=3)
        stream_data = b"a" * 25  # 25 bytes
        
        # Mock internal methods to isolate logic
        self.uploader._process_stream = MagicMock(return_value=len(stream_data))
        self.uploader._complete_upload = MagicMock()

        total_size = self.uploader.upload_stream(
            context, 
            (chunk for chunk in [stream_data]), 
            mode="ab"
        )

        # Should return offset + uploaded bytes
        self.assertEqual(total_size, 20 + 25)
        self.uploader._process_stream.assert_called_once()
        self.uploader._complete_upload.assert_called_once()
        # Should NOT abort task
        self.mock_client.abortMultipartUpload.assert_not_called()

    def test_upload_stream_wb_mode(self):
        """Test upload_stream in 'wb' (overwrite) mode."""
        context = UploadContext("test_key", "old_id", offset=50, next_part=6)
        stream_data = b"b" * 30
        
        # Mock initiateMultipartUpload for the NEW task
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.body.uploadId = "new_id"
        self.mock_client.initiateMultipartUpload.return_value = mock_resp

        self.uploader._process_stream = MagicMock(return_value=len(stream_data))
        self.uploader._complete_upload = MagicMock()

        total_size = self.uploader.upload_stream(
            context, 
            (chunk for chunk in [stream_data]), 
            mode="wb"
        )

        # 1. Should abort old task
        self.mock_client.abortMultipartUpload.assert_called_with(
            "test_bucket", "test_key", "old_id"
        )
        # 2. Should initiate new task
        self.mock_client.initiateMultipartUpload.assert_called_with(
            "test_bucket", "test_key"
        )
        # 3. Context should be updated
        self.assertEqual(context.upload_id, "new_id")
        self.assertEqual(context.offset, 0)
        self.assertEqual(context.next_part, 1)

        # 4. Result should be 0 (new offset) + 30 (uploaded)
        self.assertEqual(total_size, 30)

    def test_process_stream_logic(self):
        """Test the byte counting and part uploading logic in _process_stream."""
        # Setup mocks for actual upload
        self.mock_client.uploadPart.return_value.status = 200
        self.mock_client.uploadPart.return_value.body.etag = "etag"
        
        # 25 bytes total, part_size=10 -> should be 3 parts (10, 10, 5)
        data_chunks = [b"1234567890", b"1234567890", b"12345"]
        def iterator():
            for chunk in data_chunks:
                yield chunk
        
        key = "test_key"
        uid = "test_uid"
        start_part = 1
        
        # Temporarily mock _fetch_uploaded_parts_map to avoid network call
        self.uploader._fetch_uploaded_parts_map = MagicMock(return_value={})

        uploaded_bytes = self.uploader._process_stream(
            iterator(), key, uid, start_part, 25, 10
        )

        self.assertEqual(uploaded_bytes, 25)
        # Check if uploadPart was called 3 times
        self.assertEqual(self.mock_client.uploadPart.call_count, 3)
        
        # Check part numbers
        calls = self.mock_client.uploadPart.call_args_list
        part_nums = [c[1]['partNumber'] for c in calls]
        self.assertEqual(sorted(part_nums), [1, 2, 3])


class TestDynamicPartSizeAdjustment(unittest.TestCase):
    """测试动态分片大小调整功能"""

    def setUp(self):
        with patch('pyobs.core.ObsClient') as MockObsClient:
            self.mock_client = MockObsClient.return_value
            self.uploader = StreamUploader(
                ak="test_ak", sk="test_sk", server="test_server", bucket_name="test_bucket",
                part_size=20 * 1024 * 1024  # 20MB 默认分片大小
            )
            self.uploader.max_workers = 2
            self.uploader._process_stream = MagicMock(return_value=0)
            self.uploader._complete_upload = MagicMock()

    def test_dynamic_adjustment_with_total_size(self):
        """测试传入 total_size 时触发动态调整 (200GB)"""
        context = UploadContext("test_key", "test_id", offset=0, next_part=1)

        # 200GB 文件预估，需要的分片大小计算：
        # SAFE_PARTS_COUNT = 8000 (更保守的安全边际)
        # min_part_size = 200GB / 8000 ≈ 26.8MB > 20MB (默认)
        # 应该触发调整

        # Mock 初始化响应
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.body.uploadId = "new_upload_id"
        self.mock_client.initiateMultipartUpload.return_value = mock_resp

        # 调用 upload_stream，传入 total_size
        self.uploader.upload_stream(
            context,
            (chunk for chunk in [b""]),  # 空流
            mode="ab",
            total_size=200 * 1024**3  # 200GB
        )

        # 验证 _process_stream 被调用
        self.uploader._process_stream.assert_called_once()
        call_args = self.uploader._process_stream.call_args
        # 分片大小应该被调整为 > 20MB
        adjusted_part_size = call_args[0][5]  # part_size 参数

        # Code divides by 8000 (SAFE_PARTS_COUNT) and uses math.ceil
        import math
        expected_min_size = math.ceil(200 * 1024**3 / 8000)

        self.assertGreater(adjusted_part_size, 20 * 1024 * 1024)
        self.assertEqual(adjusted_part_size, expected_min_size)

    def test_no_adjustment_without_total_size(self):
        """测试不传 total_size 时提升至 150MB"""
        context = UploadContext("test_key", "test_id", offset=0, next_part=1)

        # Mock 初始化响应
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.body.uploadId = "new_upload_id"
        self.mock_client.initiateMultipartUpload.return_value = mock_resp

        # 调用 upload_stream，不传 total_size
        self.uploader.upload_stream(
            context,
            (chunk for chunk in [b""]),
            mode="ab"
            # 不传 total_size
        )

        # 验证 _process_stream 被调用
        self.uploader._process_stream.assert_called_once()
        call_args = self.uploader._process_stream.call_args
        # 分片大小应该提升至 150MB (支持更大文件)
        self.assertEqual(call_args[0][5], 150 * 1024 * 1024)


class TestPartNumberValidation(unittest.TestCase):
    """测试分片号校验功能"""

    def test_exceed_max_parts_raises_exception(self):
        """测试分片号超过 MAX_PARTS 时抛出异常"""
        with patch('pyobs.core.ObsClient') as MockObsClient:
            uploader = StreamUploader(
                ak="test_ak", sk="test_sk", server="test_server", bucket_name="test_bucket"
            )

            # 尝试上传分片号 10001（超过上限）
            with self.assertRaises(Exception) as context:
                uploader._upload_part_with_retry("test_key", "test_id", 10001, b"data")

            self.assertIn("10001", str(context.exception))
            self.assertIn("10000", str(context.exception))

    def test_max_parts_equal_to_limit_no_exception(self):
        """测试分片号等于 MAX_PARTS 时不抛出异常（Mock 模式）"""
        with patch('pyobs.core.ObsClient') as MockObsClient:
            mock_client = MockObsClient.return_value
            mock_client.uploadPart.return_value.status = 200
            mock_client.uploadPart.return_value.body.etag = "test_etag"

            uploader = StreamUploader(
                ak="test_ak", sk="test_sk", server="test_server", bucket_name="test_bucket"
            )

            # 分片号 10000 应该能正常上传
            result = uploader._upload_part_with_retry("test_key", "test_id", 10000, b"data")
            self.assertEqual(result, "test_etag")


if __name__ == '__main__':
    unittest.main()
