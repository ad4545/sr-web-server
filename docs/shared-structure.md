# Shared Structure

This repository keeps the HTTP API and realtime runtime separate, but both roles now use a flatter layout.

## Support Folders

Shared infrastructure helpers are grouped by concern:

- [`config/env.js`](../config/env.js): environment parsing
- [`config/constants.js`](../config/constants.js): shared constants
- [`config/grpc.js`](../config/grpc.js) and [`config/grpcUtils.js`](../config/grpcUtils.js): realtime gRPC topic config
- [`clients/mongo.js`](../clients/mongo.js), [`clients/redis.js`](../clients/redis.js), [`clients/kafka.js`](../clients/kafka.js), [`clients/rabbitmq.js`](../clients/rabbitmq.js), [`clients/s3.js`](../clients/s3.js): external clients
- [`clients/protobuf.js`](../clients/protobuf.js): protobuf schema/type loading
- [`lib/logger.js`](../lib/logger.js): structured logger factory
- [`lib/errors.js`](../lib/errors.js): application error types and controller error responses
- [`state/runtimeState.js`](../state/runtimeState.js): live runtime dependency/readiness state

## API Core

HTTP flow is now:

`index.js` -> `routes/*` -> `controllers/*` -> `models/*` -> external systems

Use this when changing:

- routes or verbs
- HTTP request/response behavior
- Mongo-backed API data access
- map cache/object-store reads

## Realtime Core

Realtime flow is now:

`index.js` -> `realtime/*` -> external systems

Use this when changing:

- websocket event handling
- gRPC framing/subscription logic
- protobuf decoding behavior
- task completion tracking
- RabbitMQ twist publishing

## Maintenance Notes

- Keep HTTP code in `routes/`, `controllers/`, and `models/` only
- Keep realtime logic inside the flat `realtime/` folder
- Keep third-party client setup in the root support files
