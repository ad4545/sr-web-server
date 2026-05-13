const express = require("express");

function createHealthRouter({ envConfig, serviceName, readinessProbe }) {
  const router = express.Router();

  router.get("/", (req, res) => {
    res.send(`Server is running at ${envConfig.ports.public}`);
  });

  router.get("/internal/health", (req, res) => {
    res.status(200).json({
      service: serviceName,
      status: "ok",
    });
  });

  router.get("/internal/ready", (req, res) => {
    const ready = readinessProbe();
    res.status(ready ? 200 : 503).json({
      service: serviceName,
      status: ready ? "ready" : "starting",
    });
  });

  return router;
}

module.exports = {
  createHealthRouter,
};
