# File providers

A file provider decides what `GET /v1/files` lists and what
`GET /v1/files/{filename}` serves. It does not affect where uploads are
written; that is always the server's output directory.

## Default: `DirectoryFileProvider`

Lists the `.msz` and `.mzML` files directly inside a directory (not
subdirectories) and resolves names only within it. Names containing `/`, `\`,
or `..` are rejected.

## Custom providers

Implement the `FileProvider` protocol, two async methods:

```python
from collections.abc import Sequence
from pathlib import Path

from mstransfer import create_app
from mstransfer.server.models import FileInfo


class ChecksumFileInfo(FileInfo):
    sha256: str


class CatalogProvider:
    def __init__(self, catalog: dict[str, tuple[Path, str]]) -> None:
        self._catalog = catalog  # name -> (path, sha256)

    async def list_files(self) -> Sequence[FileInfo]:
        return [
            ChecksumFileInfo(
                name=name,
                size_bytes=path.stat().st_size,
                format="msz",
                sha256=digest,
            )
            for name, (path, digest) in self._catalog.items()
        ]

    async def get_file(self, filename: str) -> Path | None:
        entry = self._catalog.get(filename)
        return entry[0] if entry else None


app = create_app(file_provider=CatalogProvider({...}))
```

Return `None` from `get_file` for unknown names to produce a 404. Fields added
by a `FileInfo` subclass (like `sha256` above) appear in the `/v1/files`
response.

## Serving a file under a different name

`get_file` may return a `ServedFile` instead of a `Path` when the bytes are not
stored under the name the client asked for: a content-addressed blob, a cache
entry keyed by ID, or a temp file fetched from object storage.

```python
from pathlib import Path

from mstransfer.server.models import ServedFile


class BlobProvider:
    def __init__(self, index: dict[str, Path]) -> None:
        self._index = index  # "run1.mzML" -> blobs/sha256/ab/cdef...

    async def list_files(self) -> Sequence[FileInfo]: ...

    async def get_file(self, filename: str) -> Path | ServedFile | None:
        blob = self._index.get(filename)
        return ServedFile(path=blob, name=filename) if blob else None
```

The response streams `path` and uses `name` for the `Content-Disposition`
filename and the content type (`application/xml` for `.mzML`,
`application/octet-stream` otherwise). Range requests, `If-Range`, `ETag` and
`Last-Modified` still come from the stored file, so interrupted downloads
resume as usual. A plain `Path` is served under its own name.
