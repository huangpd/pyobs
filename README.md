# sais_pyobs

[![PyPI version](https://badge.fury.io/py/sais-pyobs.svg)](https://badge.fury.io/py/sais-pyobs)
[![CI](https://github.com/huangpd/pyobs/actions/workflows/ci.yml/badge.svg)](https://github.com/huangpd/pyobs/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

一个为AI数据集设计的高性能、支持断点续传的华为云OBS流式上传工具。

该库的核心功能是实现 **从一个数据流（如HTTP URL）到OBS的直接传输**，整个过程无需将文件完整保存在本地磁盘，非常适合大文件的迁移和备份。

## 核心特性

- **流式上传 (Stream Upload)**: 直接将网络响应流、文件流等数据以流的形式上传至OBS，内存占用极低。
- **断点续传 (Breakpoint Resume)**: 如果上传过程意外中断，再次运行时能自动从上次的断点处继续上传，无需重头再来。
- **并发分片 (Concurrent Upload)**: 内部使用多线程并发上传数据分片；线程池大小可通过 `max_workers` 按机器调优。
- **配置灵活 (Flexible Configuration)**: 支持通过构造函数参数或环境变量来设置OBS的访问凭证和配置。

## 安装

你可以通过 pip 直接从 PyPI 安装：

```bash
pip install sais-pyobs --upgrade
```

该命令会自动安装所有必要的依赖项，包括：
- `requests`
- `huaweicloud-sdk-python-obs`
- `tqdm`

## 如何使用

以下是一个典型的用例：将一个网络上的大文件直接传输到你的OBS桶中。

```python
import requests
from pyobs import StreamUploader, PartLimitExceededError

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
    bucket_name="your-bucket-name",
    max_workers=8,  # 按带宽/内存调并发，默认 5
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
    raise SystemExit(0)
else:
    response.raise_for_status()

# --- total_size 必须是「完整对象大小」，不是剩余大小 ---
# HTTP 206 的 Content-Length 只是本次 Range 的剩余字节，需要加上已上传 offset。
content_length = int(response.headers.get("Content-Length", 0) or 0)
if response.status_code == 206:
    total_size = context.offset + content_length
else:
    total_size = content_length

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
except PartLimitExceededError as e:
    # 历史分片太多、剩余分片数不够时：放弃旧任务，用覆盖模式整文件重传
    print(f"分片数不足（剩余 {e.remaining_parts}，预计需要 {e.estimated_parts}），改用 mode='wb' 重传")
    response.close()
    full = requests.get(source_url, stream=True)
    full.raise_for_status()
    full_size = int(full.headers.get("Content-Length", 0) or 0)
    final_size = uploader.upload_stream(
        context=context,
        stream_iterator=full.iter_content(chunk_size=10 * 1024 * 1024),
        total_size=full_size,
        mode="wb",
    )
    print(f"✅ 覆盖重传成功！OBS 最终文件大小: {final_size} 字节")
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
    - **适用场景**: 请求了 Range 但源站强制返回了 200 (全文)；捕获 `PartLimitExceededError` 后放弃旧任务重传；或你明确希望从头开始上传。

### 2. `total_size` 语义（完整对象大小）

`total_size` **始终表示文件/对象的完整字节数**，不是「还剩多少没传」。

| 场景 | 正确写法 |
| --- | --- |
| 已知文件总大小 | 直接传入该值 |
| HTTP 200（全文） | `int(response.headers["Content-Length"])` |
| HTTP 206（Range 续传） | `context.offset + int(response.headers["Content-Length"])` |

不要把 Range 响应的 `Content-Length` 原样传进来：续传时它只是剩余大小，会导致分片计算、进度条、提前校验全部出错。若传入值小于 `context.offset`，SDK 会抛出 `ValueError`。

进度条使用真实已上传字节（`context.offset` 或服务端分片 size 之和），不会用 `(分片号 - 1) * part_size` 估算。

### 3. 并发 (`max_workers`)

构造函数参数，默认 `5`。大文件、高带宽机器可调高；内存或带宽紧张时调低，避免把本机打满。

```python
uploader = StreamUploader(max_workers=16, part_size=32 * 1024 * 1024, ...)
```

### 4. 返回值

`upload_stream` 返回一个整数，代表 **文件在 OBS 上的最终总大小**（即 `context.offset` + 本次实际上传的字节数）。这方便你直接比对文件完整性。

### 5. SSL 补丁（可选）

华为云 OBS SDK 在部分 Python 版本上创建 HTTPS 连接会因 `check_hostname` 参数报错。本库默认在 `StreamUploader` 初始化时幂等打补丁，**不会在 import 时改写全局 `ObsClient`**。

```python
from pyobs import StreamUploader, apply_obs_ssl_patch

# 关闭自动补丁（若你自行处理 SSL）
uploader = StreamUploader(..., patch_obs_ssl=False)

# 或在创建 Uploader 之前手动打补丁
apply_obs_ssl_patch()
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
    bucket_name="your-bucket-name",
    max_workers=8,
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

2.  **`upload_stream(context, stream_iterator, total_size=..., ...)`**:
    - **目的**: 接收数据流并执行上传。
    - **过程**:
        1.  用户根据上一步返回的 `context.offset` 从数据源（如URL）请求一个从 `offset` 开始的数据流。
        2.  `total_size` 传入完整文件大小；SDK 用 `total_size - context.offset` 计算剩余量并决定分片大小。
        3.  当读取的数据达到预设的分片大小（默认为20MB；未知总大小时提升至150MB）时，就将这个数据块提交到线程池中进行异步上传。
        4.  所有数据流读取完毕且所有分片都上传成功后，发送一个“完成”请求给OBS，OBS会将所有分片按顺序合并成一个完整的文件。
        5.  如果在中途失败，已上传的分片会保留在OBS上，等待下一次 `init_upload` 时被识别，从而实现续传。

## 开发

```bash
pip install -e .
python -m unittest discover -s test -v
```

变更记录见 [CHANGELOG.md](CHANGELOG.md)。
