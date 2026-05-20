# sr_web_server

`sr_web_server` runs in two roles:

- `api-core` for the HTTP API
- `realtime-core` for websocket delivery, gRPC subscriptions, and twist publishing

Both roles start from [`index.js`](index.js). The entrypoint loads env once, reads `APP_ROLE`, and boots the matching runtime.

## Documentation

- [API Core](docs/api-core.md)
- [Realtime Core](docs/realtime-core.md)
- [Shared Structure](docs/shared-structure.md)

## Run

- `npm run start:api-core`
- `npm run start:realtime-core`
- `npm run start:nginx`

## Layout

- `index.js` is the single composition root
- `routes/` contains flat HTTP routers
- `controllers/` contains flat HTTP controller functions
- `models/` contains flat HTTP data-access functions
- `realtime/` contains the flat non-HTTP realtime subsystem
- `config/` holds environment and gRPC/config helpers
- `clients/` holds external client wrappers and protobuf loaders
- `lib/` holds shared logger and error helpers
- `state/` holds runtime process state

## Rule Of Thumb

- Change an HTTP endpoint: start in `routes/`, then `controllers/`, then `models/`
- Change realtime websocket or gRPC behavior: start in `realtime/`
- Change startup or dependency wiring: start in `index.js`
