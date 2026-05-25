const path = require("path");
const http = require("http");
const dotenv = require("dotenv");
const express = require("express");
const cors = require("cors");

dotenv.config();

const { env } = require("./config/env");
const { createLogger } = require("./lib/logger");
const { createMongoConnection } = require("./clients/mongo");
const { createRedisConnection } = require("./clients/redis");
const { createKafkaClient } = require("./clients/kafka");
const { createRabbitMqClient } = require("./clients/rabbitmq");
const { createS3Client } = require("./clients/s3");
const { createAthenaClient } = require("./clients/athena");
const {
  loadProtobufSchema,
  loadStreamRouterTypes,
  loadTwistType,
} = require("./clients/protobuf");
const { createTwistHandler } = require("./realtime/twist");
const { createGrpcStreamClient } = require("./clients/grpc");
const { createRealtimeStreamDefinitions } = require("./realtime/streamDefinitions");
const { createSocketServer } = require("./realtime/socket");
const { createTaskCompletionTracker } = require("./realtime/taskCompletion");
const { setApiState, setRealtimeState, setRole } = require("./state/runtimeState");

const healthRoutes = require("./routes/health");
const tasksRoutes = require("./routes/tasks");
const pathsRoutes = require("./routes/paths");
const waypointsRoutes = require("./routes/waypoints");
const mapRoutes = require("./routes/map");
const analyticsRoutes = require("./routes/analytics");

const role = process.env.APP_ROLE;

const getApiPort = () => {
  return Number(process.env.PORT || process.env.API_CORE_PORT || 3002);
};

const getRealtimePort = () => {
  return Number(process.env.PORT || process.env.REALTIME_CORE_PORT || 3003);
};

const loadGrpcSchemas = async (grpc) => {
  const schemaEntries = await Promise.all(
    Object.entries(grpc.schemas).map(async ([name, schemaConfig]) => [name, await loadProtobufSchema(schemaConfig)])
  );

  return Object.fromEntries(schemaEntries);
};

const createRealtimeStreams = ({
  grpc,
  grpcTypes,
  logger,
  schemas,
  socket,
  taskCompletionTracker,
}) => {
  const streamDefinitions = createRealtimeStreamDefinitions({
    grpcConfig: grpc,
    logger,
    socket,
    taskCompletionTracker,
  });

  return Object.fromEntries(
    streamDefinitions.map((definition) => [
      definition.name,
      createGrpcStreamClient({
        grpcConfig: grpc,
        streamName: definition.name,
        label: definition.label,
        topics: grpc.topics[definition.topicKey],
        schema: schemas[definition.schemaKey],
        topicStreamRequestType: grpcTypes.topicStreamRequestType,
        rawDataChunkType: grpcTypes.rawDataChunkType,
        onMessage: definition.onMessage,
        logger: logger.child(`${definition.name}-stream`).child("grpc"),
      }),
    ])
  );
};

const startApiCore = async () => {
  setRole("api-core");

  const logger = createLogger("api-core");
  setApiState({
    ready: false,
    logger,
  });

  const mongo = await createMongoConnection({
    uri: env.mongo.uri,
    dbName: env.mongo.dbName,
    logger,
  });
  const redis = createRedisConnection({
    config: env.redis,
    logger,
  });
  const kafkaClient = createKafkaClient({
    config: env.kafka,
  });
  const kafkaProducer = kafkaClient.createProducer();
  await kafkaProducer.connect();
  logger.info("Kafka producer connected");

  const s3 = createS3Client({
    config: env.aws,
  });

  const athena = createAthenaClient({
    awsConfig: env.aws,
    athenaConfig: env.athena,
  });

  setApiState({
    kafkaProducer,
    mongo,
    redis,
    s3,
    athena,
  });

  const app = express();

  app.use(express.json());
  app.use(express.urlencoded({ extended: true }));
  app.use(cors({ origin: "*" }));
  app.use(express.static(path.join(__dirname, "public", "assets")));

  app.use(healthRoutes);
  app.use(waypointsRoutes);
  app.use(pathsRoutes);
  app.use(tasksRoutes);
  app.use(mapRoutes);
  app.use("/analytics", analyticsRoutes);

  const server = app.listen(getApiPort(), () => {
    setApiState({ ready: true });
    logger.info(`API core listening on ${getApiPort()}`);

    // Call dispatch once on server startup -----
    try {
      const { dispatch } = require("./dispatcher");
      dispatch().catch((err) => {
        logger.error("Error running dispatch on startup of API core", err);
      });
    } catch (err) {
      logger.error("Failed to load dispatcher on startup of API core", err);
    }
    // --------------------------------------
  });

  const shutdown = async (signal) => {
    logger.info(`Received ${signal}, shutting down API core`);
    setApiState({ ready: false });

    await new Promise((resolve, reject) => {
      server.close((error) => {
        if (error) {
          reject(error);
          return;
        }

        resolve();
      });
    });

    await Promise.allSettled([
      kafkaProducer.disconnect(),
      redis.close(),
      mongo.close(),
    ]);

    process.exit(0);
  };

  process.on("SIGINT", () => {
    shutdown("SIGINT").catch((error) => {
      logger.error("API core shutdown failed", error);
      process.exit(1);
    });
  });

  process.on("SIGTERM", () => {
    shutdown("SIGTERM").catch((error) => {
      logger.error("API core shutdown failed", error);
      process.exit(1);
    });
  });
};

const startRealtimeCore = async () => {
  setRole("realtime-core");

  const logger = createLogger("realtime-core");
  const { grpc } = require("./config/grpc");
  const rabbitMqClient = createRabbitMqClient({
    config: env.rabbitmq,
    logger: logger.child("rabbitmq"),
  });
  const s3 = createS3Client({
    config: env.aws,
  });

  setRealtimeState({
    ready: false,
    logger,
    rabbitMqClient,
    streams: {},
  });

  const [schemas, twistType, grpcTypes] = await Promise.all([
    loadGrpcSchemas(grpc),
    loadTwistType(),
    loadStreamRouterTypes(),
  ]);

  const app = express();
  app.use(healthRoutes);

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
  const taskCompletionTracker = createTaskCompletionTracker({
    s3,
    bucketName: env.aws.taskCompletionBucket,
    logger: logger.child("task-completion"),
  });

  socket.onTwist(twistHandler);

  setRealtimeState({
    socket,
    taskCompletionTracker,
  });

  rabbitMqClient.connect().catch((error) => {
    logger.error(
      "RabbitMQ startup connection failed; realtime core will continue and retry in background",
      error.message
    );
  });

  const streams = createRealtimeStreams({
    grpc,
    grpcTypes,
    logger,
    schemas,
    socket,
    taskCompletionTracker,
  });

  setRealtimeState({
    streams,
  });

  await Promise.all(Object.values(streams).map((stream) => stream.start()));

  setRealtimeState({
    ready: true,
  });

  httpServer.listen(getRealtimePort(), () => {
    logger.info(`Realtime core listening on ${getRealtimePort()}`);

    // Call dispatch once on server startup
    try {
      const { dispatch } = require("./dispatcher");
      dispatch().catch((err) => {
        logger.error("Error running dispatch on startup of Realtime core", err);
      });
    } catch (err) {
      logger.error("Failed to load dispatcher on startup of Realtime core", err);
    }
  });

  const shutdown = async (signal) => {
    logger.info(`Received ${signal}, shutting down realtime core`);
    setRealtimeState({ ready: false });

    await Promise.allSettled([
      ...Object.values(streams).map((stream) => stream.stop()),
      taskCompletionTracker.close(),
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
  };

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
};

if (!role || !["api-core", "realtime-core"].includes(role)) {
  console.error("APP_ROLE must be set to one of: api-core, realtime-core.");
  process.exit(1);
}

const startByRole = {
  "api-core": startApiCore,
  "realtime-core": startRealtimeCore,
};

startByRole[role]().catch((error) => {
  console.error(error);
  process.exit(1);
});
