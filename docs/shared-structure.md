# Shared Structure

This repository is designed around two runtime roles that share a few common utilities.

## Shared Modules

### `lib/`

Contains small reusable building blocks:

- [`lib/logger.js`](../lib/logger.js): structured logger factory
- [`lib/errors.js`](../lib/errors.js): application error types
- [`lib/validation.js`](../lib/validation.js): input validation helpers

These modules are intentionally small so both cores can depend on them without duplicating behavior.

### `clients/`

Contains infrastructure wrappers and schema loading:

- MongoDB client
- Redis client
- Kafka client
- RabbitMQ client
- S3 client
- protobuf schema loaders

The rule here is simple: code in `clients/` may know about third-party SDKs, but the rest of the application should depend on the smallest useful interface.

### `config/`

Contains environment parsing, constants, and gRPC topic/schema configuration.

- [`config/env.js`](../config/env.js) parses process environment into structured config
- [`config/constants.js`](../config/constants.js) stores shared constants and protobuf conversion options
- [`config/grpc.js`](../config/grpc.js) defines gRPC topics and protobuf schema references
- [`config/grpc-utils.js`](../config/grpc-utils.js) contains small topic helper functions

## Core Boundaries

### API Core

HTTP request in:

`index.js` -> `api/server.js` -> `api/app.js` -> `api/routes/*` -> `api/handlers/*` -> `api/services/*` -> `api/repositories/*` -> external systems

### Realtime Core

Websocket or gRPC data in:

`index.js` -> `realtime/server.js` -> `realtime/app.js` / `realtime/socket.js` / `realtime/grpc/*` -> `realtime/streams/*` -> external systems

## Where to Change What

- Add or change an API endpoint: `api/routes/` and `api/handlers/`
- Change business rules for API data: `api/services/`
- Change persistence for API data: `api/repositories/`
- Change validation for API requests: `api/validators/`
- Change websocket payloads: `realtime/socket.js` and `realtime/streams/`
- Change gRPC subscription behavior: `realtime/grpc/client.js`
- Change protobuf schema loading: `clients/protobuf.js`
- Change topic naming or schema references: `config/grpc.js`

## Maintenance Notes

- Keep code in `clients/` free of application-specific logic.
- Keep `realtime/streams/` focused on transforming decoded protobuf objects into websocket emissions.
- Keep `api/services/` focused on use-case logic and not on Express response formatting.
- Prefer thin handlers and thin route modules so the flow stays readable.
