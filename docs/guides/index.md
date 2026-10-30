# Python Guides

The `mstransfer.client` package exposes the same transfers the CLI uses.
Every function has a synchronous form and an `async_` form:

| Sync | Async | Purpose |
|------|-------|---------|
| `send_file` | `async_send_file` | Upload one file |
| `send_batch` | `async_send_batch` | Upload many files in parallel |
| `download_file` | `async_download_file` | Download one file |
| `download_batch` | `async_download_batch` | Download many files in parallel |

The sync functions call `asyncio.run()`, so they cannot be used from inside a
running event loop. Use the `async_` variants there instead.

- [Uploading files](uploading.md)
- [Downloading files](downloading.md)
- [Progress reporting](progress.md)

Full signatures are in the [API Reference](../reference/client.md).
