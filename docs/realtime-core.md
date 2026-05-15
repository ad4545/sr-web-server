# Realtime Core

The realtime core is the streaming part of the service. It handles websocket clients, gRPC subscriptions, protobuf decoding, and twist publishing.

## Purpose

The realtime core exists to:

- accept websocket connections
- stream decoded data to websocket clients
- subscribe to gRPC topic streams
- decode protobuf messages
- publish twist commands to RabbitMQ
- report health/readiness for the realtime subsystem

It should not contain HTTP CRUD logic or MongoDB repository logic.

## Startup Flow

The realtime core starts from [`index.js`](../index.js):

1. `index.js` reads `APP_ROLE`
2. If `APP_ROLE=realtime-core`, it loads [`realtime/server.js`](../realtime/server.js)
3. `realtime/server.js` loads environment config from [`config/env.js`](../config/env.js)
4. It creates the logger from [`lib/logger.js`](../lib/logger.js)
5. It creates the RabbitMQ client
6. It loads protobuf schemas from the `.proto` files
7. It creates the Express health app through [`realtime/app.js`](../realtime/app.js)
8. It creates the Socket.IO server through [`realtime/socket.js`](../realtime/socket.js)
9. It creates the twist handler and gRPC subscription streams
10. It starts the streams and begins listening on the realtime port

### Startup Responsibility Map

| File | Role |
| --- | --- |
| [`realtime/server.js`](../realtime/server.js) | Composition root for realtime core |
| [`realtime/app.js`](../realtime/app.js) | Health/readiness HTTP app |
| [`realtime/socket.js`](../realtime/socket.js) | Websocket event gateway |
| [`realtime/grpc/client.js`](../realtime/grpc/client.js) | Generic gRPC subscription client |
| [`realtime/grpc/framing.js`](../realtime/grpc/framing.js) | gRPC frame encode/decode helpers |
| [`realtime/streams/*`](../realtime/streams) | Stream adapters for decoded protobuf data |
| [`realtime/handlers/twist.js`](../realtime/handlers/twist.js) | Socket twist request adapter |
| [`realtime/services/twist.service.js`](../realtime/services/twist.service.js) | Twist publishing use-case |
| [`realtime/validators/twist.js`](../realtime/validators/twist.js) | Twist payload validation |

## Runtime Flow

The realtime flow has two major directions:

### 1. gRPC to websocket

```text
gRPC stream
  -> frame parsing
  -> protobuf decode
  -> stream adapter
  -> websocket emit
```

### 2. websocket to RabbitMQ

```text
websocket twist event
  -> twist handler
  -> twist service
  -> validation + protobuf encode
  -> RabbitMQ publish
```

## Component Roles

### `realtime/server.js`

This is the realtime composition root.

It wires together:

- environment config
- logger
- RabbitMQ client
- protobuf schema loading
- websocket server
- gRPC subscription client
- twist publishing
- readiness checks

It also owns shutdown behavior.

### `realtime/app.js`

This creates the small HTTP app for:

- `/internal/health`
- `/internal/ready`

It reports:

- whether the service is running
- whether RabbitMQ is ready
- whether gRPC streams are ready
- stream error status
- connected topic information

### `realtime/socket.js`

This is the websocket gateway.

It:

- accepts client connections
- listens for `twist` events from clients
- emits `position`, `task_feedback`, and `battery` events to all clients
- provides a clean `close()` method for shutdown

It does not know how gRPC works or how protobuf is loaded.

### `realtime/grpc/client.js`

This is the generic gRPC subscription client.

It handles:

- HTTP/2 connection setup
- topic subscription requests
- gRPC frame parsing
- protobuf decoding using the supplied schema
- reconnect loops
- status tracking
- graceful shutdown

Important point:

- it does **not** know about websocket clients
- it does **not** know about domain rules
- it does **not** know what the decoded data means

It only returns decoded protobuf-shaped data and metadata.

### `realtime/grpc/framing.js`

This file contains the gRPC message framing utilities.

Responsibilities:

- encode a payload into a gRPC message frame
- reassemble incoming byte chunks into complete frames

This logic is separate so it is easy to reuse or port to another language.

### `realtime/streams/*`

These are realtime-specific adapters.

They take decoded protobuf objects and decide what to do with them:

- `odom` -> emit `position`
- `feedback` -> emit `task_feedback`
- `battery` -> emit `battery`

These modules are where realtime-specific behavior lives.

### `realtime/handlers/twist.js`

This is the websocket input adapter.

It:

- receives a twist payload from a websocket client
- validates the payload
- passes it to the twist service
- sends an acknowledgement back to the client

### `realtime/services/twist.service.js`

This is the twist use-case.

It:

- validates the payload
- encodes the protobuf message
- publishes to RabbitMQ

The handler is thin; the service owns the behavior.

## gRPC Decoding Flow

The realtime gRPC decode flow works like this:

```text
subscription config
  -> AMR/topic list
  -> gRPC request
  -> raw gRPC bytes
  -> frame parser
  -> protobuf schema decode
  -> decoded object
  -> stream adapter
  -> websocket emit
```

### Schema loading

Schema references are defined in [`config/grpc.js`](../config/grpc.js).

Each stream has:

- `.proto` file path
- protobuf type name
- object conversion options

[`clients/protobuf.js`](../clients/protobuf.js) loads the schema and returns a usable protobuf type object.

### Decoded output

The decoded output is the canonical protobuf object for that stream.

That means:

- a consuming service can use it directly
- a consuming service can transform it
- a consuming service can store it

The client does not force a specific downstream shape.

## Stream Flows

### Odometry

```text
topic subscription
  -> raw payload
  -> decode with Odometry.proto
  -> resolve robot id from topic
  -> emit websocket position event
```

### Task Feedback

```text
topic subscription
  -> raw payload
  -> decode with Feedback.proto
  -> emit websocket task_feedback event
```

### Battery

```text
topic subscription
  -> raw payload
  -> decode with Battery.proto
  -> emit websocket battery event
```

## Ready / Health State

The realtime core reports:

- `ok` on `/internal/health`
- `starting`, `ready`, or `degraded` on `/internal/ready`

The readiness payload also includes:

- RabbitMQ readiness
- RabbitMQ error message
- gRPC readiness
- gRPC error message
- connected gRPC topics

This makes it easier to debug startup issues in production.

## Adding a New Stream

If you add a new realtime stream, follow this order:

1. Add the schema reference in [`config/grpc.js`](../config/grpc.js)
2. Load the schema in [`realtime/server.js`](../realtime/server.js)
3. Add a stream adapter in [`realtime/streams/`](../realtime/streams)
4. Wire the decoded output into `realtime/socket.js`
5. Add or update the topic suffixes in config if needed

## Maintenance Guidelines

- Keep transport code in `realtime/grpc/`
- Keep decoding and websocket emission separated
- Keep RabbitMQ publishing isolated in the twist service
- Keep the realtime server as wiring only
- Prefer small functions with obvious names over large nested callbacks

That structure makes the realtime core easier to maintain for new developers.
