const { env } = require("../config/env");
const { sendErrorResponse } = require("../lib/errors");
const { state } = require("../state/runtimeState");

const getServiceName = () => {
  return state.role === "realtime-core" ? "realtime-core" : "api-core";
};

const getRoot = async (req, res) => {
  const { params } = req;
  const { body } = req;
  const { query } = req;

  try {
    return res.send(`Server is running at ${env.ports.public}`);
  } catch (error) {
    return sendErrorResponse(res, error, state.api.logger || state.realtime.logger);
  }
};

const getHealth = async (req, res) => {
  const { params } = req;
  const { body } = req;
  const { query } = req;

  try {
    return res.status(200).json({
      service: getServiceName(),
      status: "ok",
    });
  } catch (error) {
    return sendErrorResponse(res, error, state.api.logger || state.realtime.logger);
  }
};

const getReady = async (req, res) => {
  const { params } = req;
  const { body } = req;
  const { query } = req;

  try {
    if (state.role === "realtime-core") {
      const streams = Object.values(state.realtime.streams || {});
      const grpcError = streams
        .map((stream) => stream?.getStatus?.() || null)
        .map((status) => status?.lastError || null)
        .find(Boolean) || null;

      return res.status(state.realtime.ready ? 200 : 503).json({
        service: "realtime-core",
        status: state.realtime.ready ? "ready" : "starting",
        dependencies: {
          rabbitmq: state.realtime.rabbitMqClient?.isReady?.() ? "ready" : "degraded",
          grpc: state.realtime.ready ? "ready" : "degraded",
        },
        rabbitmqError: state.realtime.rabbitMqClient?.getLastError?.()?.message || null,
        grpcError,
        grpcConnections: Object.fromEntries(
          Object.entries(state.realtime.streams || {}).map(([name, stream]) => {
            const status = stream?.getStatus?.() || {};
            return [name, status.connectedTopics || []];
          })
        ),
      });
    }

    return res.status(state.api.ready ? 200 : 503).json({
      service: "api-core",
      status: state.api.ready ? "ready" : "starting",
    });
  } catch (error) {
    return sendErrorResponse(res, error, state.api.logger || state.realtime.logger);
  }
};

module.exports = {
  getHealth,
  getReady,
  getRoot,
};
