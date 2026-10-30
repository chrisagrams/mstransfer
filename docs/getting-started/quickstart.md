# Quick Start

## 1. Start a server

On the machine that should receive files:

```bash
mstransfer serve --port 1319 --output-dir ./received --store-as msz
```

`--store-as msz` keeps files compressed. Use `--store-as mzml` to decompress
each file back to mzML once it arrives.

## 2. Upload files

On the sending machine, pass any mix of files and directories, followed by
the server address:

```bash
mstransfer upload /data/experiment1.mzML /data/batch/ remote-host:1319
```

mzML files are compressed to MSZ on the fly; `.msz` and `.mszx` files are
sent as-is. Received files land in the server's `--output-dir`.

## 3. Download files

Anything the server stores can be fetched back over HTTP:

```bash
curl http://remote-host:1319/v1/files            # list files
mstransfer download http://remote-host:1319/v1/files/experiment1.msz \
    --output-dir ./downloads --store-as mzml
```

`--store-as mzml` decompresses the downloaded MSZ locally.

## 4. Or do it from Python

```python
from pathlib import Path

from mstransfer.client import send_file

result = send_file(Path("experiment1.mzML"), "http://remote-host:1319")
print(result.state, result.stored_as)
```

## Next steps

- [CLI reference](../cli/index.md) for every flag
- [Uploading files](../guides/uploading.md) and
  [Downloading files](../guides/downloading.md) from Python
- [Authentication](../server/auth.md) to require an API key
