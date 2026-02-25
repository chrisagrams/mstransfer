# mstransfer

[![Tests](https://github.com/chrisagrams/mstransfer/actions/workflows/test.yml/badge.svg)](https://github.com/chrisagrams/mstransfer/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/chrisagrams/mstransfer/graph/badge.svg)](https://codecov.io/gh/chrisagrams/mstransfer)

Transfer mass spectrometry files (mzML / MSZ) between machines over HTTP. Files are always transported in compressed MSZ format using [mscompress](https://github.com/chrisagrams/mscompress) — mzML sources are compressed on-the-fly without temp files, and the receiving end can optionally decompress back to mzML.

## Install

```bash
pip install .
# or with uv
uv pip install .
```

## Quick start

Start a server on the receiving machine:

```bash
mstransfer serve --port 1319 --store-as msz
```

Upload files from the source machine:

```bash
mstransfer upload /data/experiment1.mzML /data/batch/ remote-host:1319
```

## Usage

### `mstransfer serve`

Start the receiver server.

```
mstransfer serve [--host 0.0.0.0] [--port 1319] [--output-dir ./received] [--store-as msz|mzml]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `0.0.0.0` | Bind address |
| `--port` | `1319` | Listen port |
| `--output-dir` | `./received` | Where received files are written |
| `--store-as` | `msz` | Store as `msz` (compressed) or `mzml` (decompress on arrival) |

### `mstransfer upload`

Upload files to a server. Accepts any mix of files and directories. The last positional argument is the target.

```
mstransfer upload <paths...> <host[:port]> [--recursive] [--parallel 4]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--recursive`, `-r` | off | Recurse into directories |
| `--parallel`, `-p` | `4` | Number of concurrent uploads |

Supported file types: `.mzML`, `.msz`, `.mszx`.

- **mzML files** are compressed to MSZ on-the-fly via `compress_stream()` and streamed directly into the HTTP request — no temp files on the sender.
- **MSZ / MSZX files** are streamed as-is.

## Programmatic access

You can use mstransfer as a library to send files from your own Python code:

```python
from pathlib import Path
from mstransfer.client import send_file, send_batch

# Send a single file (Path)
result = send_file(Path("experiment.mzML"), "http://remote-host:1319")

# Send an already-opened mscompress object
from mscompress import MZMLFile, MSZFile
from mscompress.mszx import MSZXFile

mzml = MZMLFile(b"/data/experiment.mzML")
send_file(mzml, "http://remote-host:1319")

msz = MSZFile(b"/data/experiment.msz")
send_file(msz, "http://remote-host:1319")

mszx = MSZXFile.open("/data/experiment.mszx")
send_file(mszx, "http://remote-host:1319")

# Send multiple files in parallel
send_batch(
    [Path("a.mzML"), Path("b.msz"), mszx],
    "http://remote-host:1319",
    parallel=4,
)
```

`send_file` and `send_batch` accept any mix of `Path`, `MZMLFile`, `MSZFile`, and `MSZXFile` inputs.

## API

The server exposes a REST API under `/v1/`:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/v1/health` | GET | Server status, version, storage mode |
| `/v1/upload` | POST | Upload a file (streamed MSZ body) |
| `/v1/transfer/{id}/status` | GET | Poll transfer state |

### Embedding in another app

The server is built as a FastAPI app factory:

```python
from mstransfer import create_app

# Standalone
app = create_app(output_dir="/data/ms", store_as="mzml")

# Mount in an existing FastAPI app
from fastapi import FastAPI
main_app = FastAPI()
main_app.mount("/transfer", create_app())
```

## How it works

```
Sender                                          Server
──────                                          ──────
.mzML → compress_stream() ─┐
                            ├─ MSZ bytes ──→ POST /v1/upload
.msz  → read file ─────────┘
                                                store-as msz? → write to disk
                                                store-as mzml? → write temp .msz
                                                                 → decompress
                                                                 → cleanup temp
```

## Development

```bash
uv sync --dev
uv run pytest
uv run ruff check src/ tests/
```
