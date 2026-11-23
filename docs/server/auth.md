# Authentication

Auth is off by default. When enabled, it protects every endpoint except
`GET /v1/health`.

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
