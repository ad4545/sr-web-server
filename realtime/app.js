const express = require("express");

function createRealtimeApp({ readinessState }) {
  const app = express();

  app.get("/internal/health", (req, res) => {
    res.status(200).json({
      service: "realtime-core",
      status: "ok",
    });
  });

  app.get("/internal/ready", (req, res) => {
    const state = readinessState();
    res.status(state.ready ? 200 : 503).json({
      service: "realtime-core",
      status: state.ready ? "ready" : "starting",
      dependencies: {
        rabbitmq: state.rabbitmqReady ? "ready" : "degraded",
      },
      rabbitmqError: state.rabbitmqError || null,
    });
  });

  return app;
}

module.exports = {
  createRealtimeApp,
};
