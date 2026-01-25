
import unittest
import logging
from unittest.mock import MagicMock, patch
from io import BytesIO
import sys
import os

# Ensure src is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from pyobs.core import StreamUploader, UploadContext

class TestLogThrottling(unittest.TestCase):
    def setUp(self):
        # Flatten logger to capture output
        self.log_capture = BytesIO()
        self.handler = logging.StreamHandler(self.log_capture)
        self.logger = logging.getLogger("ObsStream")
        self.logger.addHandler(self.handler)
        self.logger.setLevel(logging.WARNING)

        # Mock ObsClient
        self.patcher = patch('pyobs.core.ObsClient')
        self.MockObsClient = self.patcher.start()
        self.mock_client = self.MockObsClient.return_value
        
        self.uploader = StreamUploader(
            ak="test", sk="test", server="test", bucket_name="test"
        )
        # Set low thresholds for testing
        self.uploader.SAFETY_THRESHOLD = 2
        self.uploader.MAX_PARTS = 5  
        self.uploader.part_size = 100 # Small part size
        self.uploader.MIN_PART_SIZE = 10 

    def tearDown(self):
        self.patcher.stop()
        self.logger.removeHandler(self.handler)

    def test_log_throttling(self):
        """Verify that warning log appears only once per part"""
        context = UploadContext("key", "uid", 0, 1)
        
        # Simulate a stream that produces enough data for multiple parts
        # Total size big enough to force adjustment
        total_size = 10000 
        
        # Determine data chunks.
        # We need enough chunks to call calculate_dynamic_part_size multiple times for the SAME part.
        # If part_size is 100, and we send 10 byte chunks, we get 10 calls per part.
        chunks = [b"x"*10 for _ in range(30)] # 300 bytes total, should be 3 parts
        
        # Mock internal methods to avoid actual upload logic issues
        self.uploader._upload_part_with_retry = MagicMock(return_value="etag")
        self.uploader._complete_upload = MagicMock()
        
        self.uploader.upload_stream(
            context,
            (c for c in chunks),
            total_size=total_size, 
            mode="ab"
        )

        log_output = self.log_capture.getvalue().decode()
        print("Captured Log:\n", log_output)

        # We expect "接近阈值" to appear.
        # Since SAFETY_THRESHOLD is 2, part 2 and 3 should trigger check.
        # But for each part, it should ONLY appear ONCE.
        
        # Count occurrences of specific log message for part 2
        part_2_warnings = log_output.count("分片 2 接近阈值")
        self.assertLessEqual(part_2_warnings, 1, f"Part 2 warning should appear at most once, found {part_2_warnings}")
        
        # Count occurrences for part 3
        part_3_warnings = log_output.count("分片 3 接近阈值")
        self.assertLessEqual(part_3_warnings, 1, f"Part 3 warning should appear at most once, found {part_3_warnings}")

if __name__ == '__main__':
    unittest.main()
