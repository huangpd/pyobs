
import unittest
from unittest.mock import MagicMock, patch
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from pyobs.core import StreamUploader, UploadContext

class TestDefaultPartSize(unittest.TestCase):
    def setUp(self):
        self.patcher = patch('pyobs.core.ObsClient')
        self.MockObsClient = self.patcher.start()
        self.uploader = StreamUploader(ak="test", sk="test", server="test", bucket_name="test")
        self.uploader._process_stream = MagicMock(return_value=0)
        self.uploader._complete_upload = MagicMock()

    def tearDown(self):
        self.patcher.stop()

    def test_default_part_size_increase(self):
        """Verify that part size increases to 100MB if total_size is None"""
        context = UploadContext("key", "uid", 0, 1)
        
        self.uploader.upload_stream(context, iter([]), total_size=None)
        
        # Check calling args of _process_stream
        # Arg 5 is part_size
        call_args = self.uploader._process_stream.call_args
        part_size_arg = call_args[0][5]
        
        expected_size = 100 * 1024 * 1024
        self.assertEqual(part_size_arg, expected_size)
    
    def test_respect_larger_config(self):
        """If user configured 200MB, it should stay 200MB"""
        self.uploader.part_size = 200 * 1024 * 1024
        context = UploadContext("key", "uid", 0, 1)
        
        self.uploader.upload_stream(context, iter([]), total_size=None)
        
        call_args = self.uploader._process_stream.call_args
        part_size_arg = call_args[0][5]
        
        self.assertEqual(part_size_arg, 200 * 1024 * 1024)

if __name__ == '__main__':
    unittest.main()
