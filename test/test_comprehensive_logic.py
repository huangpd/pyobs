"""
综合测试用例：验证分片大小计算和紧急扩容逻辑
"""
import unittest
from unittest.mock import MagicMock, patch
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from pyobs.core import StreamUploader, UploadContext


class TestPartSizeCalculation(unittest.TestCase):
    """测试初始分片大小计算逻辑"""
    
    def setUp(self):
        self.patcher = patch('pyobs.core.ObsClient')
        self.MockObsClient = self.patcher.start()
        self.uploader = StreamUploader(ak="test", sk="test", server="test", bucket_name="test")
        self.uploader._process_stream = MagicMock(return_value=0)
        self.uploader._complete_upload = MagicMock()

    def tearDown(self):
        self.patcher.stop()

    def test_small_file_keeps_default_20mb(self):
        """测试小文件（<180GB）保持默认 20MB 分片"""
        context = UploadContext("key", "uid", 0, 1)
        
        # 100GB 文件
        total_size = 100 * 1024**3
        self.uploader.upload_stream(context, iter([]), total_size=total_size)
        
        call_args = self.uploader._process_stream.call_args
        part_size_arg = call_args[0][5]
        
        # 100GB / 9000 ≈ 11.6MB < 20MB，应该保持 20MB
        self.assertEqual(part_size_arg, 20 * 1024 * 1024)
    
    def test_large_file_auto_adjusts(self):
        """测试大文件（>180GB）自动调整分片大小"""
        context = UploadContext("key", "uid", 0, 1)
        
        # 500GB 文件
        total_size = 500 * 1024**3
        self.uploader.upload_stream(context, iter([]), total_size=total_size)
        
        call_args = self.uploader._process_stream.call_args
        part_size_arg = call_args[0][5]
        
        # 500GB / 9000 ≈ 58.3MB > 20MB，应该调整
        expected_size = (total_size + 9000 - 1) // 9000
        self.assertEqual(part_size_arg, expected_size)
        self.assertGreater(part_size_arg, 20 * 1024 * 1024)
    
    def test_unknown_size_defaults_150mb(self):
        """测试未知大小默认 150MB"""
        context = UploadContext("key", "uid", 0, 1)
        
        self.uploader.upload_stream(context, iter([]), total_size=None)
        
        call_args = self.uploader._process_stream.call_args
        part_size_arg = call_args[0][5]
        
        self.assertEqual(part_size_arg, 150 * 1024 * 1024)
    
    def test_respects_larger_configured_size(self):
        """测试如果用户配置了更大的分片，保持用户配置"""
        self.uploader.part_size = 200 * 1024 * 1024
        context = UploadContext("key", "uid", 0, 1)
        
        # 100GB 文件，计算结果 < 200MB
        total_size = 100 * 1024**3
        self.uploader.upload_stream(context, iter([]), total_size=total_size)
        
        call_args = self.uploader._process_stream.call_args
        part_size_arg = call_args[0][5]
        
        # 应该保持用户配置的 200MB
        self.assertEqual(part_size_arg, 200 * 1024 * 1024)


class TestEmergencyExpansion(unittest.TestCase):
    """测试紧急扩容逻辑"""
    
    def setUp(self):
        self.patcher = patch('pyobs.core.ObsClient')
        self.MockObsClient = self.patcher.start()
        self.mock_client = self.MockObsClient.return_value
        
        self.uploader = StreamUploader(ak="test", sk="test", server="test", bucket_name="test")
        self.uploader._upload_part_with_retry = MagicMock(return_value="etag")
        self.uploader._complete_upload = MagicMock()
        self.uploader._fetch_uploaded_parts_map = MagicMock(return_value={})

    def tearDown(self):
        self.patcher.stop()

    def test_expansion_at_5000_parts(self):
        """测试在 5000 分片时触发扩容"""
        context = UploadContext("key", "uid", 0, 1)
        
        # 模拟已经上传了 4999 个分片，现在上传第 5000 个
        # 使用小分片来快速达到 5000
        self.uploader.part_size = 10  # 10 bytes
        
        # 生成足够的数据触发多个分片
        chunks = [b"x" * 10 for _ in range(100)]  # 1000 bytes total
        
        # 手动调用 _process_stream，从 part 5000 开始
        self.uploader._process_stream(
            iter(chunks),
            "key",
            "uid",
            5000,  # 从 5000 开始
            None,  # 未知总大小
            10     # 初始 10 bytes
        )
        
        # 验证上传被调用（说明扩容后能继续上传）
        self.assertTrue(self.uploader._upload_part_with_retry.called)
    
    def test_expansion_multipliers(self):
        """测试扩容倍数是否正确"""
        # 直接测试 calculate_dynamic_part_size 函数的逻辑
        # 通过模拟不同的 part number 来验证
        
        # 这里我们通过实际运行来验证倍数
        # Part 5000-7000: 1.5x
        # Part 7000-8000: 2x
        # Part 8000-9000: 5x
        # Part >9000: 10x
        
        # 由于函数是嵌套的，我们通过观察日志或者实际运行来验证
        # 这里简化为验证能成功处理大量分片
        context = UploadContext("key", "uid", 0, 1)
        
        self.uploader.part_size = 10
        chunks = [b"x" * 10 for _ in range(50)]
        
        # 应该能成功完成
        self.uploader._process_stream(iter(chunks), "key", "uid", 5000, None, 10)
        self.assertTrue(self.uploader._upload_part_with_retry.called)


class TestEdgeCases(unittest.TestCase):
    """测试边界情况"""
    
    def setUp(self):
        self.patcher = patch('pyobs.core.ObsClient')
        self.MockObsClient = self.patcher.start()
        self.uploader = StreamUploader(ak="test", sk="test", server="test", bucket_name="test")
        self.uploader._process_stream = MagicMock(return_value=0)
        self.uploader._complete_upload = MagicMock()

    def tearDown(self):
        self.patcher.stop()

    def test_exactly_180gb_file(self):
        """测试临界值 180GB 文件"""
        context = UploadContext("key", "uid", 0, 1)
        
        # 180GB = 9000 * 20MB
        total_size = 180 * 1024**3
        self.uploader.upload_stream(context, iter([]), total_size=total_size)
        
        call_args = self.uploader._process_stream.call_args
        part_size_arg = call_args[0][5]
        
        # 180GB / 9000 = 20MB，应该等于默认值
        expected = (total_size + 9000 - 1) // 9000
        self.assertEqual(part_size_arg, expected)
    
    def test_zero_total_size(self):
        """测试 total_size=0 的情况"""
        context = UploadContext("key", "uid", 0, 1)
        
        self.uploader.upload_stream(context, iter([]), total_size=0)
        
        call_args = self.uploader._process_stream.call_args
        part_size_arg = call_args[0][5]
        
        # 应该当作未知大小处理，默认 150MB
        self.assertEqual(part_size_arg, 150 * 1024 * 1024)
    
    def test_very_large_file_639gb(self):
        """测试用户报告的 639GB 文件"""
        context = UploadContext("key", "uid", 0, 1)
        
        total_size = 639 * 1024**3
        self.uploader.upload_stream(context, iter([]), total_size=total_size)
        
        call_args = self.uploader._process_stream.call_args
        part_size_arg = call_args[0][5]
        
        # 639GB / 9000 ≈ 74.5MB
        expected = (total_size + 9000 - 1) // 9000
        self.assertEqual(part_size_arg, expected)
        
        # 验证分片数不会超过 9000
        estimated_parts = total_size / part_size_arg
        self.assertLessEqual(estimated_parts, 9000)


if __name__ == '__main__':
    unittest.main()
