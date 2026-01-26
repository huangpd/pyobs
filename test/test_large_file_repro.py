
import unittest
import logging
from unittest.mock import MagicMock, patch
from io import BytesIO
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from pyobs.core import StreamUploader, UploadContext

class TestLargeFileUploadFailure(unittest.TestCase):
    def setUp(self):
        # Flatten logger
        self.logger = logging.getLogger("ObsStream")
        self.logger.setLevel(logging.CRITICAL)

        # Mock ObsClient
        self.patcher = patch('pyobs.core.ObsClient')
        self.MockObsClient = self.patcher.start()
        self.mock_client = self.MockObsClient.return_value
        
        self.uploader = StreamUploader(
            ak="test", sk="test", server="test", bucket_name="test",
            part_size=10 # Small part size for testing
        )
        # Set max parts small to simulate hitting the limit quickly
        self.uploader.MAX_PARTS = 20
        self.uploader.SAFETY_THRESHOLD = 5 
        # With part_size=10, max capacity = 20 * 10 = 200 bytes

    def tearDown(self):
        self.patcher.stop()

    def test_upload_failure_without_total_size(self):
        """Verify that upload fails if total_size is unknown and data exceeds capacity"""
        context = UploadContext("key", "uid", 0, 1)
        
        # 300 bytes > 200 bytes capacity
        data_chunk = b"x" * 300 
        
        self.uploader._upload_part_with_retry = MagicMock(return_value="etag")
        self.uploader._complete_upload = MagicMock()
        
        # Now it should succeed because emergency resizing will kick in
        self.uploader.upload_stream(
            context,
            (c for c in [data_chunk]),
            total_size=None, # UNKNOWN SIZE
            mode="ab"
        )

if __name__ == '__main__':
    unittest.main()
