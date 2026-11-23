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

The body streams into a `.part` file next to the destination and is renamed
into place only once complete and exactly the size the server announced, so
an interrupted download never leaves a truncated file at the destination.
Missing parent directories are created.

## Resuming

By default (`resume=True`), an interrupted download keeps its `.part` file,
plus a `.part.json` describing it, and the next call for the same URL and
destination continues from where it stopped with a `Range` request. A partial
is kept only when the server sent a strong `ETag` and the body size, and it is
resumed with `If-Range` carrying that `ETag`, so a file that changed on the
server is downloaded again from the start. A server that ignores ranges also
just sends the whole file again.

Only one download to a destination at a time owns the resumable partial; it
holds a lock on it. Any concurrent download to the same destination writes to
its own private `.part` file, which is never resumed.

- `resume=False` starts from zero and keeps nothing on failure. On success it
  also removes a partial an earlier download left, unless another download is
  using it.
- `force=True` discards any partial before downloading.

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

## Timeouts

Without `client=`, requests use a 10 s connect timeout and 300 s otherwise.
`timeout=` takes an `httpx.Timeout` that replaces those (or a passed client's)
for the download; `httpx.Timeout(None)` disables them.

## Many files

```python
from pathlib import Path

from mstransfer.client import DownloadRequest, download_batch

base = "http://remote-host:1319/v1/files"
requests = [
    DownloadRequest(url=f"{base}/{name}", dest=Path("downloads") / name)
    for name in ("a.msz", "b.msz")
]
results = download_batch(requests, store_as="mzml", parallel=4)

for r in results:
    if r.error is not None:
        print(f"{r.filename} failed: {r.error}")
    elif r.response.skipped:
        print(f"{r.filename} already exists at {r.response.path}")
    else:
        print(f"{r.filename} saved to {r.response.path}")
```

`download_batch` never raises for a single failed file. It returns one
[`FileResult`](../reference/client.md#mstransfer.client.results.FileResult)
per request, in input order and named after the request's `dest`. Each holds
either a
[`DownloadResult`](../reference/client.md#mstransfer.client.downloader.DownloadResult)
(`request`, the final `path`, and `skipped`) in `response`, or the exception in
`error`. A multi-member `.mszx` yields its archive directory as the path.

## Authentication

Pass `api_key=` to send `Authorization: Bearer <key>`:

```python
download_file(url, dest, api_key="s3cret")
```
