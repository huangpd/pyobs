# Changelog

本项目遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循 [SemVer](https://semver.org/lang/zh-CN/)。

## [0.3.0] - 2026-09-22

### 高优先级

- **`max_workers` 可配置**：`StreamUploader(..., max_workers=8)`，默认仍为 5。大文件可按机器带宽/内存调优，避免写死线程池把资源打满。
- **`total_size` 语义写死为「完整对象大小」**：不再和「剩余大小」混用。断点续传时剩余量始终按 `total_size - context.offset` 计算。若传入值小于已上传字节，会抛出 `ValueError` 提示应使用 `context.offset + Content-Length`。
- **进度条使用真实已上传字节**：`tqdm.initial` 优先用 `context.offset`，否则用 `listParts` 的实际 size 之和；不再用 `(start_part - 1) * part_size` 估算（中途调整过分片大小时会偏）。

### 中优先级

- 分片上传成功日志从 INFO 降为 DEBUG，避免成百上千个分片刷屏。失败/重试仍为 WARNING。
- SSL 猴子补丁不再在 `import pyobs` 时改写全局 `ObsClient`。默认在 `StreamUploader.__init__` 里幂等应用，可用 `patch_obs_ssl=False` 关闭，或手动调用 `apply_obs_ssl_patch()`。
- `abortMultipartUpload` 等处不再 `except Exception: pass`，至少打一行 WARNING，便于排查。

### 低优先级 / 工程化

- 公开接口补充 type hints（Python ≥ 3.10）。
- 增加 GitHub Actions：`python -m unittest discover`，矩阵 3.10–3.13。
- README 补充 `PartLimitExceededError` 后 `mode="wb"` 的错误处理示例；`pyproject.toml` 邮箱改为真实地址。

## [0.2.6] - 此前

- 空数据流（Content-Length=0）时静默跳过上传，不再抛出异常。
- 动态分片、分片超限自定义异常、断点续传等既有能力。
