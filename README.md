# sr_web_server

`sr_web_server` is split into two long-running roles:

- `api-core` for REST-style HTTP endpoints and database/object-store access
- `realtime-core` for websocket delivery and gRPC subscriptions

The code is intentionally organized so the two roles can be reasoned about independently, while still sharing common helpers for logging, validation, configuration, and protobuf loading.

## Documentation

- [API Core](docs/api-core.md)
- [Realtime Core](docs/realtime-core.md)
- [Shared Structure](docs/shared-structure.md)

## Quick Run

The runtime entrypoint is [`index.js`](index.js). It reads `APP_ROLE` and starts the matching service:

- `APP_ROLE=api-core` starts [`api/server.js`](api/server.js)
- `APP_ROLE=realtime-core` starts [`realtime/server.js`](realtime/server.js)

## High-Level Layout

- `api/` contains HTTP routes, handlers, services, repositories, and validators
- `realtime/` contains websocket handling, gRPC subscription code, stream adapters, and twist publishing
- `clients/` contains infrastructure clients and schema loaders
- `config/` contains environment parsing and shared constants
- `lib/` contains shared logger, errors, and validation utilities
- `middlewares/` contains Express middleware helpers

## Maintenance Rule of Thumb

- If you are changing an HTTP endpoint, start in `api/routes/` and follow the chain down to `api/services/`.
- If you are changing websocket or gRPC behavior, start in `realtime/server.js` and follow the chain into `realtime/grpc/` and `realtime/streams/`.
- If you are adding a new external dependency, prefer wrapping it in `clients/` so the rest of the app talks to a smaller interface.
