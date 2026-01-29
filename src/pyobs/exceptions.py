# ==========================================
# 🚨 自定义异常类
# ==========================================


class UploadError(Exception):
    """OBS 上传基础异常类"""
    pass


class PartLimitExceededError(UploadError):
    """分片数超限异常，需要清理旧任务并使用更大的分片重新上传"""
    
    def __init__(self, message, remaining_parts=None, estimated_parts=None, remaining_size=None, part_size=None):
        super().__init__(message)
        self.remaining_parts = remaining_parts  # 剩余可用分片数
        self.estimated_parts = estimated_parts  # 预估需要的分片数
        self.remaining_size = remaining_size    # 剩余需上传的数据量 (bytes)
        self.part_size = part_size              # 当前分片大小 (bytes)


class PartUploadError(UploadError):
    """单个分片上传失败异常"""
    
    def __init__(self, message, part_number=None):
        super().__init__(message)
        self.part_number = part_number
