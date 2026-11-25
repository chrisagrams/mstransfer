# Authentication

Auth is off by default. When enabled, it protects every endpoint except
`GET /v1/health`. Uploads and downloads can also be protected separately; see
[Protecting uploads or downloads only](#protecting-uploads-or-downloads-only).

## API key

From the CLI:

```bash
mstransfer serve --api-key s3cret
mstransfer upload data/ remote-host:1319 --api-key s3cret
mstransfer download http://remote-host:1319/v1/files/run1.msz --api-key s3cret
```

Each command falls back to the `MS_API_KEY` environment variable when
`--api-key` isn't given; see [CLI](../cli/index.md#api-key).

From Python:

```python
from mstransfer import APIKeyAuthProvider, create_app

app = create_app(auth=APIKeyAuthProvider("s3cret"))
```

`APIKeyAuthProvider` accepts the key as `Authorization: Bearer <key>` or as an
`?api_key=<key>` query parameter, compares it in constant time, and returns
401 otherwise. Keys may contain non-ASCII characters; they are compared as
UTF-8 bytes. The Python client's upload and download functions take an
`api_key=` argument that sends the header.

## Custom providers

Any object with an async `authenticate(request)` method returning an
`AuthContext` satisfies the `AuthProvider` protocol. Raise `HTTPException` to
reject the request:

```python
from fastapi import HTTPException, Request

from mstransfer import AuthContext, create_app


class HeaderTokenProvider:
    def __init__(self, tokens: dict[str, str]) -> None:
        self._tokens = tokens  # token -> user

    async def authenticate(self, request: Request) -> AuthContext:
        user = self._tokens.get(request.headers.get("X-Token", ""))
        if user is None:
            raise HTTPException(status_code=401, detail="Bad token")
        return AuthContext(identity=user, scopes={"upload"})


app = create_app(auth=HeaderTokenProvider({"abc": "alice"}))
```

`create_app(auth=...)` also accepts a plain FastAPI dependency callable, so
you can reuse your app's existing auth dependency. For `make_router`, wrap a
provider with `make_auth_dependency(provider)`.

## Protecting uploads or downloads only

`auth` protects every route except `/v1/health`. To protect the two
directions separately, pass `upload_auth` and/or `download_auth` instead:

| Group | Routes |
|-------|--------|
| upload | `POST /v1/upload`, `GET /v1/transfer/{id}/status` |
| download | `GET /v1/files`, `GET /v1/files/{filename}` |

A group with no auth is open. For example, a server that publishes files to
anyone but only accepts uploads from trusted clients:

```python
from mstransfer import APIKeyAuthProvider, create_app

app = create_app(upload_auth=APIKeyAuthProvider("s3cret"))
```

Each accepts the same values as `auth`, so the two directions can also use
different keys or providers. `auth` can't be combined with either one;
doing so raises `TypeError`.

`make_router` takes the same split as `upload_auth_dep` and
`download_auth_dep`, alongside `auth_dep`. The OpenAPI schema documents a
401 response only on routes that have auth.
