# Downloading files

`download_file` and `download_batch` fetch any HTTP URL, typically a server's
[`/v1/files/{filename}`](../server/http-api.md#get-v1filesfilename) endpoint.

## A single file

```python
from pathlib import Path

from mstransfer.client import download_file

path = download_file(
    "http://remote-host:1319/v1/files/experiment.msz",
    Path("downloads/experiment.msz"),
)
```

The body streams into `<dest>.part` and is renamed to the destination only
once complete, so an interrupted download never leaves a truncated file in
place. Missing parent directories are created.

## Converting on download

`store_as=` converts the file after download with mscompress. The source
format comes from the URL's extension, and the destination's extension is
adjusted to match the target:

```python
path = download_file(
    "http://remote-host:1319/v1/files/experiment.msz",
    Path("downloads/experiment.msz"),
    store_as="mzml",
)
# path == Path("downloads/experiment.mzML")
```

| Source | `store_as="msz"` | `store_as="mzml"` | `store_as="mszx"` |
|--------|------------------|-------------------|-------------------|
| `.msz` | kept as-is | decompressed | `NotImplementedError` |
| `.mzML` | compressed | kept as-is | `NotImplementedError` |
| `.mszx` | MSZ extracted | decompressed | kept as-is |

If the URL has no recognizable extension, the file is saved without
conversion.

### Multi-member `.mszx` archives

An `.mszx` can bundle several MSZ files. When one with more than one member
is stored as `msz` or `mzml`, every member is written into a directory named
after the archive:

```
downloads/batch.mszx  →  downloads/batch/run1.mzML
                         downloads/batch/run2.mzML
```

Members are extracted into a staging directory that replaces `batch/` only
once all of them succeed, so a failure leaves no partial output. If two
members would map to the same file name (compared case-insensitively),
`ValueError` is raised before anything is written. The function returns the
directory path.

## Skipping existing files

- `skip_existing=True` returns early when the final destination (or, for a
  multi-member `.mszx`, its directory) already exists.
- `force=True` always downloads, overriding `skip_existing`.

## Many files

```python
from pathlib import Path

from mstransfer.client import download_batch
from mstransfer.client.downloader import DownloadRequest

base = "http://remote-host:1319/v1/files"
requests = [
    DownloadRequest(url=f"{base}/{name}", dest=Path("downloads") / name)
    for name in ("a.msz", "b.msz")
]
paths = download_batch(requests, store_as="mzml", parallel=4)
```

Failed downloads are logged and left out of the returned list rather than
raised.

## Authentication

The download functions do not take an `api_key`. To reach a server that
requires one, pass a client with the header set:

```python
import httpx

from mstransfer.client import async_download_file


async def main() -> None:
    headers = {"Authorization": "Bearer s3cret"}
    async with httpx.AsyncClient(headers=headers) as client:
        await async_download_file(url, dest, client=client)
```
