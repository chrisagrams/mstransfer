# CLI

The `mstransfer` command has three subcommands:

| Command | Purpose |
|---------|---------|
| [`serve`](#mstransfer-serve) | Run the receiving server |
| [`upload`](#mstransfer-upload) | Upload files to a server |
| [`download`](#mstransfer-download) | Download files from URLs |

`python -m mstransfer` is equivalent to `mstransfer`. `mstransfer --version`
(or `-V`) prints the installed version.

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
| `--api-key` | `$MS_API_KEY` | Require this API key on every request except `/v1/health` |

The command exits immediately if the port is already in use. Connections a
previous server left in `TIME-WAIT` don't count, so a restarted server can
rebind the port right away.

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
| `--api-key` | `$MS_API_KEY` | API key sent as `Authorization: Bearer <key>` |

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
any files that failed, with the server's reason when it gave one (for example
`409 Conflict: run1.msz already exists on the server`). A file the server refuses
because its output already exists
([409 Conflict](../server/http-api.md#post-v1upload)) counts as failed, and the
command exits non-zero if any file failed.

## `mstransfer download`

Download one or more files from URLs, such as a server's
[`/v1/files/{filename}`](../server/http-api.md#get-v1filesfilename) endpoint.

```
mstransfer download <urls...> [--output-dir ./downloads] [--parallel 4]
                    [--chunk-size 1048576] [--store-as msz|mzml|mszx]
                    [--skip-existing] [--force] [--no-resume] [--api-key KEY]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--output-dir`, `-o` | `./downloads` | Directory to save files into |
| `--parallel`, `-p` | `4` | Number of concurrent downloads |
| `--chunk-size` | `1048576` | Download chunk size in bytes |
| `--store-as` | keep as-is | Convert each file to `msz`, `mzml`, or `mszx` after downloading |
| `--skip-existing` | off | Skip files whose destination already exists |
| `--force` | off | Re-download even if the destination exists (overrides `--skip-existing`), discarding any partial download |
| `--no-resume` | off | Start interrupted downloads over instead of resuming them |
| `--api-key` | `$MS_API_KEY` | API key sent as `Authorization: Bearer <key>` |

The local file name is the last segment of the URL path. The source format is
inferred from its extension. See [Downloading files](../guides/downloading.md)
for the supported conversions and how multi-member `.mszx` archives are
handled.

If a download is interrupted (a dropped connection, Ctrl-C), running the same
command again continues from the bytes already received. If the file changed
on the server in the meantime, or the server can't serve byte ranges, the
download starts over. See [Resuming](../guides/downloading.md#resuming).

The summary counts downloaded, skipped, and failed files separately, and lists
each failure with the server's reason when it gave one. The command exits
non-zero if any file failed.

## API key

`serve`, `upload`, and `download` all take `--api-key`. When it isn't given,
each command reads the `MS_API_KEY` environment variable instead. If neither is
set (or `MS_API_KEY` is empty), no key is used.

```bash
export MS_API_KEY=s3cret
mstransfer serve                                        # requires the key
mstransfer upload run1.mzML remote-host                 # sends the key
mstransfer download http://remote-host:1319/v1/files/run1.msz
```

## Exit status

| Code | Meaning |
|------|---------|
| 0 | Every file succeeded (skipped downloads count as success) |
| 1 | At least one file failed, the server was unreachable, or the port was in use |
| 130 | Interrupted with Ctrl-C |
