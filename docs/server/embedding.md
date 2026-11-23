# Embedding in FastAPI

## Standalone app

`create_app` returns a configured FastAPI app with the router mounted at
`/v1`:

```python
import uvicorn

from mstransfer import create_app

app = create_app(output_dir="/data/ms", store_as="mzml")
uvicorn.run(app, host="0.0.0.0", port=1319)
```

| Argument | Default | Description |
|----------|---------|-------------|
| `output_dir` | `./received` | Where uploads are stored (created if missing) |
| `store_as` | `"msz"` | `"msz"` or `"mzml"` |
| `auth` | `None` | An [`AuthProvider`](auth.md), a raw FastAPI dependency, or `None` for no auth |
| `file_provider` | `None` | A [`FileProvider`](file-providers.md); defaults to scanning `output_dir` |

## Mounting as a sub-application

```python
from fastapi import FastAPI

from mstransfer import create_app

main_app = FastAPI()
main_app.mount("/transfer", create_app(output_dir="/data/ms"))
# Endpoints are now at /transfer/v1/...
```

## Including the router

To share your app's middleware, dependencies, and OpenAPI schema, include
the router directly. Pass an `AppState` so the router does not depend on
your app's `app.state`:

```python
from pathlib import Path

from fastapi import FastAPI

from mstransfer import AppState, make_router
from mstransfer.server.fileprovider import DirectoryFileProvider

out = Path("/data/ms")
out.mkdir(parents=True, exist_ok=True)

state = AppState(output_dir=out, store_as="msz", files=DirectoryFileProvider(out))

app = FastAPI()
app.include_router(make_router(state=state), prefix="/transfer/v1")
```

`make_router(auth_dep=...)` takes a FastAPI dependency to protect every route
except `/health`. Build one from a provider with `make_auth_dependency`; see
[Authentication](auth.md).

Uploads are written to `state.output_dir` regardless of the file provider,
so create that directory before serving.
