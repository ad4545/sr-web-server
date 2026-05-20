# API Core

The API core is the HTTP-facing role. It serves task, path, waypoint, map, and health endpoints.

## Startup Flow

When `APP_ROLE=api-core`, [`index.js`](../index.js) does all startup work:

1. loads env once
2. creates MongoDB, Redis, Kafka, and S3 clients
3. creates the Express app
4. registers middleware
5. mounts health and feature routers
6. starts listening on the API port

## Structure

HTTP flow is flat:

```text
index.js
  -> routes/*
  -> controllers/*
  -> models/*
  -> external systems
```

### `routes/`

Route files define paths and methods only:

- [`routes/tasks.js`](../routes/tasks.js)
- [`routes/paths.js`](../routes/paths.js)
- [`routes/waypoints.js`](../routes/waypoints.js)
- [`routes/map.js`](../routes/map.js)
- [`routes/health.js`](../routes/health.js)

### `controllers/`

Controller files keep the existing request parsing, validation, branching, and response shapes:

- [`controllers/tasks.js`](../controllers/tasks.js)
- [`controllers/paths.js`](../controllers/paths.js)
- [`controllers/waypoints.js`](../controllers/waypoints.js)
- [`controllers/map.js`](../controllers/map.js)
- [`controllers/health.js`](../controllers/health.js)

### `models/`

Model files hold all HTTP-side data access:

- [`models/tasks.js`](../models/tasks.js)
- [`models/paths.js`](../models/paths.js)
- [`models/waypoints.js`](../models/waypoints.js)
- [`models/map.js`](../models/map.js)

## Feature Routes

- `POST /save-task`
- `GET /get-all-tasks`
- `GET /get-tasks`
- `POST /send-task`
- `DELETE /delete-task/:title`
- `POST /save-path`
- `PATCH /update-path/:pathName`
- `GET /get-paths`
- `POST /save-waypoint`
- `GET /get-all-waypoints`
- `GET /get-map`
- `GET /`
- `GET /internal/health`
- `GET /internal/ready`

## Maintenance Notes

- Keep route files limited to path and verb definitions
- Keep request parsing, validation, branching, and responses in `controllers/`
- Keep MongoDB and map storage access in `models/`
- Keep startup and shutdown wiring in `index.js`
