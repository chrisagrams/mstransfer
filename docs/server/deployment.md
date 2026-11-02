# Deployment

## Docker

The repository's `Dockerfile` installs mstransfer with uv and runs
`mstransfer serve`. Any extra arguments are passed to `serve`:

```bash
docker build -t mstransfer .
docker run -p 1319:1319 -v /data/received:/data mstransfer \
    --output-dir /data --store-as msz --api-key s3cret
```

Each release also publishes `latest` and `<version>` images for
`linux/amd64` and `linux/arm64` to Docker Hub.

## Behind a reverse proxy

- Uploads are single long-lived streaming requests, so raise the proxy's
  body size limit and read timeouts accordingly, and disable request
  buffering so bytes reach the server as they arrive.
- Clients wait for the upload response until storage finishes, including
  mzML decompression. If a proxy cuts that request off, the transfer can
  still be checked via `/v1/transfer/{id}/status`.
- Transfer state is held in memory per process. Run a single worker, or route
  a client's status polls to the process that received its upload.

## TLS

The server speaks plain HTTP. For TLS, terminate it at a reverse proxy and
point clients at `https://host:port`; the upload target accepts full URLs.
