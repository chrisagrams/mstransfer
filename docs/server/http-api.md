# HTTP API

All endpoints live under `/v1`. When the server has an
[auth provider](auth.md), every endpoint except `/v1/health` requires
credentials.

FastAPI also serves interactive docs at `/docs` and the OpenAPI schema at
`/openapi.json`.

## `GET /v1/health`

Server status. Never requires auth.

```json
{"status": "ok", "version": "0.2.3", "store_as": "msz"}
```

## `POST /v1/upload`

Upload one file as a streamed request body of MSZ bytes.

| Header | Required | Description |
|--------|----------|-------------|
| `X-Transfer-ID` | yes | Client-chosen unique ID (the client uses a UUID4) |
| `X-Original-Filename` | yes | Original file name; its stem names the stored file |
| `X-Source-Format` | no | `mzML`, `msz`, or `mszx` (sent by the client, informational) |
| `Content-Type` | no | `application/octet-stream` |

The body is written to `<output-dir>/<stem>.msz`. With `store-as mzml`, it is
then decompressed to `<stem>.mzML` and the `.msz` is removed. The response
arrives once storage finishes:

```json
{
  "transfer_id": "6f1c…",
  "filename": "experiment.mzML",
  "stored_as": "received/experiment.mzML",
  "state": "done",
  "bytes_received": 104857600
}
```

| Status | Meaning |
|--------|---------|
| 200 | Stored. `state` is `done`, or `error` if decompression failed |
| 400 | Missing `X-Transfer-ID` or `X-Original-Filename` |
| 401 | Auth failed |
| 500 | Reading the body or writing the file failed |

## `GET /v1/transfer/{transfer_id}/status`

The registry record for a transfer, so a client can check on an upload from
another connection.

```json
{
  "transfer_id": "6f1c…",
  "filename": "experiment.mzML",
  "state": "decompressing",
  "bytes_received": 104857600,
  "stored_as": "",
  "error": null,
  "created_at": "2026-10-29T12:00:00"
}
```

`state` is one of `receiving`, `received`, `decompressing`, `done`, `error`.
Returns 404 if the ID is unknown or has been evicted.

## `GET /v1/files`

Files available for download, as reported by the server's
[file provider](file-providers.md). The default lists every `.msz` and
`.mzML` file directly inside the output directory, sorted by name.

```json
{
  "files": [
    {"name": "experiment.msz", "size_bytes": 20971520, "format": "msz"}
  ]
}
```

Custom providers may add fields to each entry; they are passed through.

## `GET /v1/files/{filename}`

Stream one file. `Content-Type` is `application/xml` for mzML and
`application/octet-stream` otherwise, with `Content-Length` and a
`Content-Disposition: attachment` header. Returns 404 if the provider does not
resolve the name. The default provider rejects names containing `/`, `\`, or
`..`.
