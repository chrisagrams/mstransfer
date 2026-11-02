# Progress reporting

Progress callbacks receive byte deltas, throttled to roughly one call per
8 MiB plus a final flush, so they are cheap to drive a UI from.

## Uploads

`send_file` takes a plain callable:

```python
send_file(path, server, progress_callback=lambda delta: print(delta))
```

`send_batch` takes an object implementing
[`BatchProgressCallback`](../reference/client.md#mstransfer.client.sender.BatchProgressCallback):

```python
from pathlib import Path

from mstransfer.server.models import UploadResponse


class Printer:
    def file_started(
        self, index: int, file_path: Path, total_bytes: int | None
    ) -> None:
        print(f"[{index}] start {file_path.name} ({total_bytes or '?'} bytes)")

    def file_progress(self, index: int, delta: int) -> None: ...

    def file_done(self, index: int, result: UploadResponse) -> None:
        print(f"[{index}] {result.state.value}")

    def file_error(self, index: int, exc: Exception) -> None:
        print(f"[{index}] failed: {exc}")


send_batch(paths, server, progress=Printer())
```

`total_bytes` is the file size for MSZ / MSZX and `None` for mzML, whose
compressed size is not known until the upload ends.

## Downloads

`download_file` takes a
[`DownloadProgressCallback`](../reference/client.md#mstransfer.client.downloader.DownloadProgressCallback),
an object with two methods:

- `on_total(total_bytes)`: called once when the server responds, with the
  body size from `Content-Length`, or `None` if the server didn't send one or
  the response is content-encoded.
- `on_progress(bytes_delta)`: called per chunk.

```python
class Printer:
    def on_total(self, total_bytes: int | None) -> None:
        print(f"downloading {total_bytes or '?'} bytes")

    def on_progress(self, bytes_delta: int) -> None: ...


download_file(url, dest, progress_callback=Printer())
```

`download_batch` takes a
[`BatchDownloadProgress`](../reference/client.md#mstransfer.client.downloader.BatchDownloadProgress)
with `on_file_start`, `on_file_total`, `on_file_progress`, `on_file_complete`,
and `on_file_error`, each keyed by the destination file name.
`on_file_start` always receives `total_bytes=None`, because the size isn't
known until the `GET` response arrives; `on_file_total` then reports it, with
the same rules as `on_total`.

The CLI's Rich progress bars are implementations of these protocols; see
`UploadProgressDisplay` and `DownloadProgressDisplay` in `mstransfer/cli.py`.
