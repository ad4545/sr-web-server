const http = require("http");
const { env } = require("../config/env");
const { createLogger } = require("../lib/logger");
const { createRabbitMqClient } = require("../clients/rabbitmq");
const {
  loadBatteryType,
  loadFeedbackType,
  loadOdometryType,
  loadStreamRouterTypes,
  loadTwistType,
} = require("../clients/protobuf");
const { createTwistHandler } = require("./handlers/twist");
const { createSocketServer } = require("./socket");
const { createOdomStream } = require("./streams/odom");
const { createTaskFeedbackStream } = require("./streams/feedback");
const { createBatteryStream } = require("./streams/battery");
const { createRealtimeApp } = require("./app");

async function startRealtimeCore() {
  const logger = createLogger("realtime-core");
  const { grpc } = require("../config/grpc");
  const rabbitMqClient = createRabbitMqClient({
    config: env.rabbitmq,
    logger: logger.child("rabbitmq"),
  });
  let streams = {};

  const [odometryType, feedbackType, batteryType, twistType, grpcTypes] = await Promise.all([
    loadOdometryType(),
    loadFeedbackType(),
    loadBatteryType(),
    loadTwistType(),
    loadStreamRouterTypes(),
  ]);

  let ready = false;
  const app = createRealtimeApp({
    readinessState: () => ({
      ready,
      rabbitmqReady: rabbitMqClient.isReady(),
      rabbitmqError: rabbitMqClient.getLastError()?.message || null,
      grpcReady: ready,
      grpcError: (() => {
        const statuses = Object.values(streams).map((stream) => stream?.getStatus?.() || null);
        return statuses.map((status) => status?.lastError || null).find(Boolean) || null;
      })(),
      grpcConnections: Object.fromEntries(
        Object.entries(streams).map(([name, stream]) => {
          const status = stream?.getStatus?.() || {};
          return [name, status.connectedTopics || []];
        })
      ),
    }),
  });

  const httpServer = http.createServer(app);
  const socket = createSocketServer({
    httpServer,
    logger: logger.child("socket"),
  });

  const twistHandler = createTwistHandler({
    rabbitMqClient,
    rabbitConfig: env.rabbitmq,
    twistType,
    logger: logger.child("twist"),
  });

  socket.onTwist(twistHandler);

  rabbitMqClient.connect().catch((error) => {
    logger.error(
      "RabbitMQ startup connection failed; realtime core will continue and retry in background",
      error.message
    );
  });

  streams = {
    odom: createOdomStream({
      grpcConfig: grpc,
      grpcTypes,
      odometryType,
      logger: logger.child("odom-stream"),
      emitPosition: socket.emitPosition,
    }),
    taskFeedback: createTaskFeedbackStream({
      grpcConfig: grpc,
      grpcTypes,
      feedbackType,
      logger: logger.child("task-feedback-stream"),
      emitTaskFeedback: socket.emitTaskFeedback,
    }),
    battery: createBatteryStream({
      grpcConfig: grpc,
      grpcTypes,
      batteryType,
      logger: logger.child("battery-stream"),
      emitBattery: socket.emitBattery,
    }),
  };

  await Promise.all(Object.values(streams).map((stream) => stream.start()));

  ready = true;

  httpServer.listen(env.ports.realtime, () => {
    logger.info(`Realtime core listening on ${env.ports.realtime}`);
  });

  async function shutdown(signal) {
    logger.info(`Received ${signal}, shutting down realtime core`);
    ready = false;

    await Promise.allSettled([
      ...Object.values(streams).map((stream) => stream.stop()),
      rabbitMqClient.close(),
      socket.close(),
    ]);

    await new Promise((resolve, reject) => {
      httpServer.close((error) => {
        if (error) {
          reject(error);
          return;
        }

        resolve();
      });
    });

    process.exit(0);
  }

  process.on("SIGINT", () => {
    shutdown("SIGINT").catch((error) => {
      logger.error("Realtime core shutdown failed", error);
      process.exit(1);
    });
  });

  process.on("SIGTERM", () => {
    shutdown("SIGTERM").catch((error) => {
      logger.error("Realtime core shutdown failed", error);
      process.exit(1);
    });
  });
}

startRealtimeCore().catch((error) => {
  console.error(error);
  process.exit(1);
});
