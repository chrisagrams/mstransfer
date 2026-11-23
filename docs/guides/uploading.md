# Uploading files

## A single file

`send_file` accepts a `Path` or an already-opened mscompress object:

```python
from pathlib import Path

from mscompress import MSZFile, MZMLFile
from mscompress.mszx import MSZXFile

from mstransfer.client import send_file

server = "http://remote-host:1319"

send_file(Path("experiment.mzML"), server)

send_file(MZMLFile(b"/data/experiment.mzML"), server)
send_file(MSZFile(b"/data/experiment.msz"), server)
send_file(MSZXFile.open("/data/experiment.mszx"), server)
```

For a `Path`, the format is detected from the file contents. mzML is
compressed on the fly with `compress_stream()`; MSZ and MSZX are streamed
as-is. Anything else raises `ValueError`.

`send_file` returns an
[`UploadResponse`](../reference/models.md#mstransfer.server.models.UploadResponse).
If the server is still processing the file when the upload finishes (for
example, decompressing to mzML), the client polls
[`/v1/transfer/{id}/status`](../server/http-api.md#get-v1transfertransfer_idstatus)
until it reaches `DONE` or `ERROR`, and returns that final state.

```python
from mstransfer.server.models import TransferState

result = send_file(Path("experiment.mzML"), server)
if result.state is TransferState.DONE:
    print("stored at", result.stored_as)
```

HTTP errors raise `httpx.HTTPStatusError`, including the
[409 Conflict](../server/http-api.md#post-v1upload) the server returns when
the file's output already exists there. The server's reason is in the
response body as `{"detail": "..."}`.

Two timeouts apply:

- `timeout=` takes an `httpx.Timeout` for the upload request, replacing the
  client's. Without it, the client's timeouts are used (10 s connect, 3600 s
  otherwise, when the function creates its own client).
  `httpx.Timeout(None)` disables them.
- `poll_timeout=` is how many seconds polling may go without server-side
  progress (default 3600) before raising `TimeoutError`. `None` waits forever.

## Many files

`send_batch` uploads with bounded parallelism and never raises for a single
failed file, including one that is missing or unreadable. Instead, each input
gets a
[`FileResult`](../reference/client.md#mstransfer.client.results.FileResult)
in input order, holding either the final `UploadResponse` in `response` or the
exception in `error`:

```python
from mstransfer.client import resolve_inputs, send_batch

paths = resolve_inputs(["/data/batch", "/data/extra.msz"], recursive=True)
results = send_batch(paths, server, parallel=4)

for r in results:
    if r.error is not None:
        print(f"{r.filename} failed: {r.error}")
```

`resolve_inputs` expands files and directories the same way
[`mstransfer upload`](../cli/index.md#mstransfer-upload) does, and raises
`FileNotFoundError` if nothing valid is found.

## Authentication

Pass `api_key=` to send `Authorization: Bearer <key>` on the upload and
status requests (the batch functions take it too):

```python
send_file(Path("experiment.mzML"), server, api_key="s3cret")
```

## Async and connection reuse

```python
import httpx

from mstransfer.client import async_send_batch


async def main() -> None:
    async with httpx.AsyncClient(timeout=httpx.Timeout(3600, connect=10)) as client:
        await async_send_batch(paths, server, client=client)
```

Without `client=`, each call opens and closes its own `httpx.AsyncClient`.
