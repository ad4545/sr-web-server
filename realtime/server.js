const http = require("http");
const { env } = require("../config/env");
const { createLogger } = require("../lib/logger");
const { createKafkaClient } = require("../clients/kafka");
const { createRabbitMqClient } = require("../clients/rabbitmq");
const {
  loadBatteryType,
  loadFeedbackType,
  loadOdometryType,
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
  const kafkaClient = createKafkaClient({
    config: env.kafka,
  });
  const rabbitMqClient = createRabbitMqClient({
    config: env.rabbitmq,
    logger: logger.child("rabbitmq"),
  });

  const [odometryType, feedbackType, batteryType, twistType] = await Promise.all([
    loadOdometryType(),
    loadFeedbackType(),
    loadBatteryType(),
    loadTwistType(),
  ]);

  let ready = false;
  const app = createRealtimeApp({
    readinessState: () => ({
      ready,
      rabbitmqReady: rabbitMqClient.isReady(),
      rabbitmqError: rabbitMqClient.getLastError()?.message || null,
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

  const odomStream = createOdomStream({
    kafkaClient,
    kafkaConfig: env.kafka,
    odometryType,
    logger: logger.child("odom-stream"),
    emitPosition: socket.emitPosition,
  });
  const taskFeedbackStream = createTaskFeedbackStream({
    kafkaClient,
    kafkaConfig: env.kafka,
    feedbackType,
    logger: logger.child("task-feedback-stream"),
    emitTaskFeedback: socket.emitTaskFeedback,
  });
  const batteryStream = createBatteryStream({
    kafkaClient,
    kafkaConfig: env.kafka,
    batteryType,
    logger: logger.child("battery-stream"),
    emitBattery: socket.emitBattery,
  });

  await Promise.all([
    odomStream.start(),
    taskFeedbackStream.start(),
    batteryStream.start(),
  ]);

  ready = true;

  httpServer.listen(env.ports.realtime, () => {
    logger.info(`Realtime core listening on ${env.ports.realtime}`);
  });

  async function shutdown(signal) {
    logger.info(`Received ${signal}, shutting down realtime core`);
    ready = false;

    await Promise.allSettled([
      odomStream.stop(),
      taskFeedbackStream.stop(),
      batteryStream.stop(),
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
