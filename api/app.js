const express = require("express");
const cors = require("cors");
const { createErrorHandler } = require("../middlewares/error-handler");
const { createHealthRouter } = require("./routes/health");
const { createTasksRouter } = require("./routes/tasks");
const { createPathsRouter } = require("./routes/paths");
const { createWaypointsRouter } = require("./routes/waypoints");
const { createMapRouter } = require("./routes/map");

function createApiApp({ envConfig, logger, handlers, readinessProbe }) {
  const app = express();

  app.use(express.json());
  app.use(cors({ origin: "*" }));

  app.use(
    createHealthRouter({
      envConfig,
      serviceName: "api-core",
      readinessProbe,
    })
  );
  app.use(createWaypointsRouter({ handler: handlers.waypoints }));
  app.use(createPathsRouter({ handler: handlers.paths }));
  app.use(createTasksRouter({ handler: handlers.tasks }));
  app.use(createMapRouter({ handler: handlers.map }));

  app.use(createErrorHandler(logger));

  return app;
}

module.exports = {
  createApiApp,
};
