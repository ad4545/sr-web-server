# Realtime Core

The realtime core handles websocket clients, gRPC topic subscriptions, protobuf decoding, twist publishing, and task completion tracking.

## Startup Flow

When `APP_ROLE=realtime-core`, [`index.js`](../index.js) does all startup work:

1. loads env once
2. creates the RabbitMQ and S3 clients
3. loads protobuf schemas
4. creates the health/readiness HTTP app
5. creates the Socket.IO server
6. wires twist publishing
7. starts gRPC streams
8. begins listening on the realtime port

## Structure

Realtime code stays flat in [`realtime/`](../realtime):

- [`realtime/socket.js`](../realtime/socket.js)
- [`realtime/twist.js`](../realtime/twist.js)
- [`realtime/streamDefinitions.js`](../realtime/streamDefinitions.js)
- [`realtime/taskCompletion/index.js`](../realtime/taskCompletion/index.js)

The gRPC client resides in:
- [`clients/grpc/index.js`](../clients/grpc/index.js) (stream client lifecycle)
- [`clients/grpc/framing.js`](../clients/grpc/framing.js) (gRPC framing parsing)
- [`clients/grpc/errors.js`](../clients/grpc/errors.js) (gRPC error handling)

## Runtime Flow

### gRPC to websocket

```text
gRPC stream
  -> clients/grpc/framing.js
  -> clients/grpc/index.js
  -> protobuf decode
  -> realtime/streamDefinitions.js
  -> websocket emit
```

### websocket to RabbitMQ

```text
socket "twist"
  -> realtime/twist.js
  -> protobuf encode
  -> RabbitMQ publish
```

## Health and Readiness

Realtime exposes:

- `GET /internal/health`
- `GET /internal/ready`

The readiness payload reports:

- overall realtime readiness
- RabbitMQ readiness and error state
- gRPC stream error state
- connected gRPC topics by stream

## Maintenance Notes

- Keep startup wiring in `index.js`
- Keep stream-specific routing in `realtime/streamDefinitions.js`
- Keep websocket gateway behavior in `realtime/socket.js`
- Keep gRPC connection lifecycle in `clients/grpc/index.js`
- Keep task completion upload logic in `realtime/taskCompletion/index.js`
