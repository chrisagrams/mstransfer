# CLI

The `mstransfer` command has three subcommands:

| Command | Purpose |
|---------|---------|
| [`serve`](#mstransfer-serve) | Run the receiving server |
| [`upload`](#mstransfer-upload) | Upload files to a server |
| [`download`](#mstransfer-download) | Download files from URLs |

`python -m mstransfer` is equivalent to `mstransfer`.

## `mstransfer serve`

Start the receiver server.

```
mstransfer serve [--host 0.0.0.0] [--port 1319] [--output-dir ./received]
                 [--store-as msz|mzml] [--api-key KEY]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `0.0.0.0` | Bind address |
| `--port` | `1319` | Listen port |
| `--output-dir` | `./received` | Where received files are written (created if missing) |
| `--store-as` | `msz` | Store as `msz` (compressed) or `mzml` (decompress on arrival) |
| `--api-key` | none | Require this API key on every request except `/v1/health` |

The command exits immediately if the port is already in use.

## `mstransfer upload`

Upload files to a server. Accepts any mix of files and directories; the last
positional argument is the target.

```
mstransfer upload <paths...> <target> [--recursive] [--parallel 4]
                  [--chunk-size 1048576] [--api-key KEY]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--recursive`, `-r` | off | Recurse into directories |
| `--parallel`, `-p` | `4` | Number of concurrent uploads |
| `--chunk-size` | `1048576` | Upload chunk size in bytes |
| `--api-key` | none | API key sent as `Authorization: Bearer <key>` |

**Target formats**

| Target | Resolves to |
|--------|-------------|
| `host` | `http://host:1319` |
| `host:port` | `http://host:port` |
| `http://host:port` | used as-is |
| `https://host:port` | used as-is |

**Inputs**

- Files with the extensions `.mzML`, `.msz`, or `.mszx` are uploaded; other
  files are skipped with a warning.
- Directories are scanned for `.mzML` and `.msz` files (recursively with
  `-r`).
- mzML files are compressed to MSZ on the fly and streamed straight into the
  request body. MSZ and MSZX files are streamed as-is.

Before uploading, the command checks `GET /v1/health` and exits if the
server is unreachable. When the batch finishes it prints a summary and lists
any files that failed.

## `mstransfer download`

Download one or more files from URLs, such as a server's
[`/v1/files/{filename}`](../server/http-api.md#get-v1filesfilename) endpoint.

```
mstransfer download <urls...> [--output-dir ./downloads] [--parallel 4]
                    [--chunk-size 1048576] [--store-as msz|mzml|mszx]
                    [--skip-existing] [--force]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--output-dir`, `-o` | `./downloads` | Directory to save files into |
| `--parallel`, `-p` | `4` | Number of concurrent downloads |
| `--chunk-size` | `1048576` | Download chunk size in bytes |
| `--store-as` | keep as-is | Convert each file to `msz`, `mzml`, or `mszx` after downloading |
| `--skip-existing` | off | Skip files whose destination already exists |
| `--force` | off | Re-download even if the destination exists (overrides `--skip-existing`) |

The local file name is the last segment of the URL path. The source format is
inferred from its extension. See [Downloading files](../guides/downloading.md)
for the supported conversions and how multi-member `.mszx` archives are
handled.
