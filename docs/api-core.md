# API Core

The API core is the HTTP-facing part of the service. It serves CRUD-style endpoints for tasks, paths, waypoints, and map data.

## Purpose

The API core exists to:

- accept HTTP requests
- validate incoming payloads
- coordinate business rules
- persist data in MongoDB
- publish task commands to Kafka
- fetch map data from S3 and optionally cache it in Redis

It should not contain websocket logic or gRPC subscription logic.

## Startup Flow

The API core starts from [`index.js`](../index.js):

1. `index.js` reads `APP_ROLE`
2. If `APP_ROLE=api-core`, it loads [`api/server.js`](../api/server.js)
3. `api/server.js` loads environment config from [`config/env.js`](../config/env.js)
4. It creates the logger from [`lib/logger.js`](../lib/logger.js)
5. It creates external clients:
   - MongoDB
   - Redis
   - Kafka
   - S3
6. It builds repositories and services for each feature area
7. It creates the Express app through [`api/app.js`](../api/app.js)
8. It starts listening on the API port

### Startup Responsibility Map

| File | Role |
| --- | --- |
| [`api/server.js`](../api/server.js) | Composition root for the API core |
| [`api/app.js`](../api/app.js) | Express app setup and route mounting |
| [`api/routes/*`](../api/routes) | HTTP route definitions |
| [`api/handlers/*`](../api/handlers) | HTTP request/response adapter layer |
| [`api/services/*`](../api/services) | Business/use-case logic |
| [`api/repositories/*`](../api/repositories) | MongoDB persistence layer |
| [`api/validators/*`](../api/validators) | Payload validation logic |

## Request Flow

The flow is intentionally layered:

```text
HTTP request
  -> Express route
  -> Handler
  -> Service
  -> Repository
  -> External system
```

The handler is responsible for translating HTTP into a function call and translating the result back into a response.

The service is responsible for business logic.

The repository is responsible for data access.

## Component Roles

### `api/server.js`

This is the API composition root.

It wires together:

- config
- logger
- database clients
- cache client
- publisher client
- services
- handlers
- Express app

It also owns shutdown behavior for the API process.

### `api/app.js`

This creates the Express application and mounts middleware/routes.

Responsibilities:

- JSON body parsing
- CORS configuration
- health/readiness routes
- feature routes
- centralized error handling

### Routes

Routes define URL paths and HTTP methods only.

Files:

- [`api/routes/tasks.js`](../api/routes/tasks.js)
- [`api/routes/paths.js`](../api/routes/paths.js)
- [`api/routes/waypoints.js`](../api/routes/waypoints.js)
- [`api/routes/map.js`](../api/routes/map.js)
- [`api/routes/health.js`](../api/routes/health.js)

Routes should not contain business logic.

### Handlers

Handlers adapt HTTP requests to service calls.

They are responsible for:

- reading `req.body`, `req.params`, and `req.query`
- calling the correct service method
- returning status codes and JSON payloads

Handlers in this codebase:

- [`api/handlers/tasks.js`](../api/handlers/tasks.js)
- [`api/handlers/paths.js`](../api/handlers/paths.js)
- [`api/handlers/waypoints.js`](../api/handlers/waypoints.js)
- [`api/handlers/map.js`](../api/handlers/map.js)

### Services

Services contain the use-case logic.

They decide:

- what to validate
- what to save
- how to page results
- when to publish to Kafka
- when to throw not-found errors
- how to combine cache and object storage for the map

Services in this codebase:

- [`api/services/tasks.service.js`](../api/services/tasks.service.js)
- [`api/services/paths.service.js`](../api/services/paths.service.js)
- [`api/services/waypoints.service.js`](../api/services/waypoints.service.js)
- [`api/services/map.service.js`](../api/services/map.service.js)

### Repositories

Repositories isolate MongoDB access.

They keep Mongo-specific calls like:

- `insertOne`
- `find`
- `updateOne`
- `updateMany`
- `deleteOne`

inside one place.

Repositories in this codebase:

- [`api/repositories/tasks.repository.js`](../api/repositories/tasks.repository.js)
- [`api/repositories/paths.repository.js`](../api/repositories/paths.repository.js)
- [`api/repositories/waypoints.repository.js`](../api/repositories/waypoints.repository.js)

### Validators

Validators keep input checks out of the service logic.

They verify:

- required fields exist
- arrays are not empty
- strings are non-empty
- nested objects are present
- task/path/waypoint shapes are valid

Validators in this codebase:

- [`api/validators/tasks.js`](../api/validators/tasks.js)
- [`api/validators/paths.js`](../api/validators/paths.js)
- [`api/validators/waypoints.js`](../api/validators/waypoints.js)

### Middleware

Middleware handles cross-cutting concerns:

- [`middlewares/async-handler.js`](../middlewares/async-handler.js) wraps async route handlers
- [`middlewares/error-handler.js`](../middlewares/error-handler.js) converts errors to HTTP responses

## Feature Flows

### Tasks

```text
POST /save-task
  -> validate task payload
  -> save task document in MongoDB
  -> return inserted id

GET /get-all-tasks
  -> list all tasks from MongoDB
  -> return 404 if empty

GET /get-tasks?page=N
  -> count tasks
  -> fetch one page
  -> return paging metadata

POST /send-task
  -> validate publish payload
  -> publish message to Kafka
  -> return success response

DELETE /delete-task/:title
  -> delete task by masterTaskName
  -> throw NotFoundError if missing
```

### Paths

```text
POST /save-path
  -> validate path payload
  -> save path in MongoDB

PATCH /update-path/:pathName
  -> update matching path
  -> return 404 if nothing matched

GET /get-paths
  -> list all paths
```

### Waypoints

```text
POST /save-waypoint
  -> validate waypoint payload
  -> save waypoint in MongoDB
  -> update neighbor links when needed

GET /get-all-waypoints
  -> list all waypoints
```

### Map

```text
GET /get-map
  -> try Redis cache first
  -> if cache hit, return cached buffer
  -> otherwise fetch from S3
  -> cache the result
  -> return buffer
```

## Errors and Responses

API errors are handled centrally by [`middlewares/error-handler.js`](../middlewares/error-handler.js).

Behavior:

- `AppError` subclasses return their configured status code
- unknown errors return `500`
- validation errors return `400`
- not-found errors return `404`

This keeps handlers small and avoids repeated try/catch blocks.

## Adding a New API Feature

If you add a new API endpoint, follow this order:

1. Add or update the route in `api/routes/`
2. Add the handler in `api/handlers/`
3. Add the business logic in `api/services/`
4. Add data access in `api/repositories/` if MongoDB is involved
5. Add input validation in `api/validators/`
6. Wire it in [`api/server.js`](../api/server.js)

## Maintenance Guidelines

- Keep HTTP-specific code in handlers.
- Keep business rules in services.
- Keep database calls in repositories.
- Keep validation in validators.
- Keep `api/server.js` as the wiring layer only.

That separation is what makes the API core easier to understand and safer to change.
