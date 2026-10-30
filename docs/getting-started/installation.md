# Installation

mstransfer requires Python 3.10 or newer.

## As a CLI tool

Install it as an isolated tool so the `mstransfer` command is on your `PATH`:

=== "uv"

    ```bash
    uv tool install mstransfer
    ```

=== "pipx"

    ```bash
    pipx install mstransfer
    ```

## As a library

Add it to a project to use the [Python client](../guides/index.md) or
[embed the server](../server/embedding.md):

=== "uv"

    ```bash
    uv add mstransfer
    ```

=== "pip"

    ```bash
    pip install mstransfer
    ```

## Docker

The repository ships a `Dockerfile` that runs `mstransfer serve`. See
[Deployment](../server/deployment.md).

## Verify

```bash
mstransfer --help
```
