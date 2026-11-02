# API Reference

Generated from the source docstrings.

- [Client](client.md): `mstransfer.client` upload and download functions
- [Server](server.md): `create_app`, `make_router`, auth, file providers, state
- [Models](models.md): Pydantic request/response models and `TransferState`
- [Storage](store.md): `MSFormat` and `StorePlan`, the `store_as` conversion logic

The top-level `mstransfer` package re-exports the server API: `create_app`,
`make_router`, `AppState`, `AuthProvider`, `AuthContext`, `NoAuthProvider`,
`APIKeyAuthProvider`, and `make_auth_dependency`.
