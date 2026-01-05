import sys
import os
import unittest
from io import BytesIO
from unittest.mock import MagicMock, patch

# Add src to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from pyobs.core import StreamUploader, UploadContext

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
            iterator(), key, uid, start_part, total_size=25
        )

        self.assertEqual(uploaded_bytes, 25)
        # Check if uploadPart was called 3 times
        self.assertEqual(self.mock_client.uploadPart.call_count, 3)
        
        # Check part numbers
        calls = self.mock_client.uploadPart.call_args_list
        part_nums = [c[1]['partNumber'] for c in calls]
        self.assertEqual(sorted(part_nums), [1, 2, 3])

if __name__ == '__main__':
    unittest.main()
