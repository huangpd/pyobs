# sais-pyobs

## Project Overview

**sais-pyobs** is a high-performance Python library designed for streaming data directly to Huawei Cloud OBS (Object Storage Service). It is particularly optimized for handling large AI datasets and file migrations without requiring local disk storage.

**Key Features:**
*   **Stream Upload:** Direct transfer from HTTP streams (or other iterables) to OBS, minimizing memory usage.
*   **Breakpoint Resume:** Automatically detects interrupted uploads and resumes from the last successful part.
*   **Concurrency:** Multi-threaded part uploading for high throughput.
*   **Flexible Config:** Supports configuration via environment variables or direct arguments.

## Architecture & Core Components

The project follows a standard `src`-layout structure.

*   **`src/pyobs/core.py`**: Contains the core logic.
    *   `StreamUploader`: The main class handling the upload lifecycle.
    *   `UploadContext`: A data class maintaining the state between initialization and upload phases (e.g., upload ID, current offset).
    *   **Monkey Patching**: Includes a patch for `huaweicloud-sdk-python-obs` to resolve SSL connection issues (`_patched_get_server_connection`).
*   **`src/pyobs/__init__.py`**: Exposes the `StreamUploader` class.

**Workflow:**
1.  **Initialize (`init_upload`)**: Checks OBS for existing multipart uploads for the given key. Returns an offset to resume from.
2.  **Stream (`upload_stream`)**: Reads from the input stream, buffers data into chunks (default 20MB), and uploads them concurrently using a thread pool.
3.  **Complete**: Merges all parts on OBS upon successful stream termination.

## Development Setup

### Prerequisites
*   Python >= 3.10

### Installation

To install dependencies and the package in editable mode:

```bash
pip install -e .
```

### Configuration

The library uses the following environment variables for credentials (recommended for dev/prod):

*   `OBS_AK`: Access Key ID
*   `OBS_SK`: Secret Access Key
*   `OBS_SERVER`: OBS Endpoint (e.g., `obs.cn-east-3.myhuaweicloud.com`)
*   `OBS_BUCKET`: Target Bucket Name

## Testing

The project uses the standard `unittest` framework.

**Running Tests:**

```bash
python -m unittest discover test
```

*   **Test File**: `test/test_stream_uploader.py`
*   **Mocking**: Tests use `unittest.mock` to simulate `ObsClient` behavior, avoiding actual network calls to Huawei Cloud during testing.

## Build & Release

The project uses `pyproject.toml` for build configuration.

**Build Wheel/Sdist:**

```bash
pip install build
python -m build
```

This will generate artifacts in the `dist/` directory.
