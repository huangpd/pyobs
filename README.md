# sais_pyobs

[![PyPI version](https://badge.fury.io/py/sais-pyobs.svg)](https://badge.fury.io/py/sais-pyobs)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

一个为AI数据集设计的高性能、支持断点续传的华为云OBS流式上传工具。

该库的核心功能是实现 **从一个数据流（如HTTP URL）到OBS的直接传输**，整个过程无需将文件完整保存在本地磁盘，非常适合大文件的迁移和备份。

## 核心特性

- **流式上传 (Stream Upload)**: 直接将网络响应流、文件流等数据以流的形式上传至OBS，内存占用极低。
- **断点续传 (Breakpoint Resume)**: 如果上传过程意外中断，再次运行时能自动从上次的断点处继续上传，无需重头再来。
- **并发分片 (Concurrent Upload)**: 内部使用多线程并发上传数据分片，大幅提升大文件上传速度。
- **配置灵活 (Flexible Configuration)**: 支持通过构造函数参数或环境变量来设置OBS的访问凭证和配置。

## 安装

首先，请确保你安装了 Python 3.10 或更高版本。

1.  克隆本项目到本地：
    ```bash
    git clone <your-repo-url>
    cd sais_pyobs
    ```

2.  使用 pip 从本地源码安装：
    ```bash
    pip install .
    ```
    该命令会自动处理并安装所有在 `pyproject.toml` 中声明的依赖项，包括：
    - `requests`
    - `huaweicloud-sdk-python-obs`
    - `tqdm`

## 如何使用

以下是一个典型的用例：将一个网络上的大文件直接传输到你的OBS桶中。

```python
import requests
from pyobs import StreamUploader

# --- 1. 初始化上传器 ---
# 建议使用环境变量配置，更安全
# os.environ["OBS_AK"] = "YOUR_ACCESS_KEY"
# os.environ["OBS_SK"] = "YOUR_SECRET_KEY"
# os.environ["OBS_SERVER"] = "obs.cn-east-3.myhuaweicloud.com"
# os.environ["OBS_BUCKET"] = "your-bucket-name"
# uploader = StreamUploader()

# 或者直接通过参数传入
uploader = StreamUploader(
    ak="YOUR_ACCESS_KEY",
    sk="YOUR_SECRET_KEY",
    server="obs.cn-east-3.myhuaweicloud.com",
    bucket_name="your-bucket-name"
)

# --- 2. 初始化上传任务，获取上传上下文 ---
# 这一步会检查OBS上是否有未完成的任务，并返回断点位置
object_key = "my-remote-folder/large_file.zip"
context = uploader.init_upload(object_key=object_key)

print(f"-> SDK 建议从 {context.offset} 字节处开始下载")

# --- 3. 根据断点位置发起流式下载 ---
# 使用 Range 请求头，告诉服务器只发送我们需要的数据
headers = {
    "Range": f"bytes={context.offset}-"
}
source_url = "https://example.com/path/to/your/large_file.zip"
response = requests.get(source_url, headers=headers, stream=True)

# --- 4. 智能处理 HTTP 状态码并执行上传 ---
# 根据源站对 Range 的支持情况决定上传模式
if response.status_code == 206:
    # 源站支持 Range，使用追加模式 (ab)
    mode = "ab"
    print(f"-> 源站支持续传，将从 {context.offset} 字节处继续写入")
elif response.status_code == 200:
    # 源站不支持 Range，返回了全文，必须使用覆盖模式 (wb)
    mode = "wb"
    print("-> 源站不支持续传，将重置任务并全量写入")
elif response.status_code == 416:
    print("-> 文件已完整，无需重复下载")
    return
else:
    response.raise_for_status()

# 获取本次请求的流长度，用于显示进度条
total_size = int(response.headers.get('content-length', 0))

# --- 5. 将数据流送入上传器 ---
# SDK内部会自动处理分片、并发和重试
try:
    final_size = uploader.upload_stream(
        context=context,
        stream_iterator=response.iter_content(chunk_size=10 * 1024 * 1024),
        total_size=total_size,
        mode=mode
    )
    print(f"✅ 文件上传成功！OBS 最终文件大小: {final_size} 字节")
except Exception as e:
    print(f"❌ 上传过程中断: {e}")
    print("程序已中断。下次运行时，将自动从断点续传。")

```

## 高级功能说明

### 1. 模式选择 (`mode`)

`upload_stream` 方法支持 `mode` 参数，用于处理源站（HTTP）和目的站（OBS）之间的状态同步：

- **`mode="ab"` (默认)**: 
    - **含义**: Append Binary (追加模式)。
    - **逻辑**: 直接使用 `context.upload_id` 继续上传后续分片。
    - **适用场景**: 成功获取到 HTTP 206 响应。
- **`mode="wb"`**: 
    - **含义**: Write Binary (覆盖模式)。
    - **逻辑**: 内部会自动调用 `abortMultipartUpload` 销毁旧的 `upload_id` 及其碎片，然后重新初始化一个全新的上传任务。
    - **适用场景**: 请求了 Range 但源站强制返回了 200 (全文)，或者你明确希望从头开始上传。

### 2. 返回值

`upload_stream` 现在返回一个整数，代表 **文件在 OBS 上的最终总大小**（即 `context.offset` + 本次实际上传的字节数）。这方便你直接比对文件完整性。

```

## 配置方式

你可以通过两种方式来配置 `StreamUploader`：

### 1. 构造函数参数（推荐用于测试）

在创建 `StreamUploader` 实例时直接传入所有必要信息。

```python
uploader = StreamUploader(
    ak="YOUR_ACCESS_KEY",
    sk="YOUR_SECRET_KEY",
    server="obs.cn-east-3.myhuaweicloud.com",
    bucket_name="your-bucket-name"
)
```

### 2. 环境变量（推荐用于生产）

将凭证信息存储在环境变量中，代码无需硬编码任何敏感信息。

```bash
export OBS_AK="YOUR_ACCESS_KEY"
export OBS_SK="YOUR_SECRET_KEY"
export OBS_SERVER="obs.cn-east-3.myhuaweicloud.com"
export OBS_BUCKET="your-bucket-name"
```

然后在代码中无参初始化即可：

```python
uploader = StreamUploader()
```

## 工作原理解析

本工具的工作流程分为两个核心步骤，以实现高效的断点续传：

1.  **`init_upload(object_key)`**:
    - **目的**: 准备上传并检查断点。
    - **过程**:
        1.  向OBS查询指定 `object_key` 是否存在一个尚未完成的“分段上传”（Multipart Upload）任务。
        2.  如果存在，则列出所有已成功上传的分片（Part），并计算这些分片的总字节数。这个总和就是 `offset`。
        3.  如果没有找到任务，就为这个 `object_key` 初始化一个新的分段上传任务，此时 `offset` 为 0。
        4.  返回一个包含任务ID、`offset` 等信息的 `UploadContext` 对象。

2.  **`upload_stream(context, stream_iterator, ...)`**:
    - **目的**: 接收数据流并执行上传。
    - **过程**:
        1.  用户根据上一步返回的 `context.offset` 从数据源（如URL）请求一个从 `offset` 开始的数据流。
        2.  `upload_stream` 接收这个流，并开始读取数据。
        3.  当读取的数据达到预设的分片大小（默认为20MB）时，就将这个数据块提交到一个线程池中进行异步上传。
        4.  所有数据流读取完毕且所有分片都上传成功后，发送一个“完成”请求给OBS，OBS会将所有分片按顺序合并成一个完整的文件。
        5.  如果在中途失败，已上传的分片会保留在OBS上，等待下一次 `init_upload` 时被识别，从而实现续传。
