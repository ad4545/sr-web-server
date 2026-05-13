const { env } = require("../config/env");
const { createLogger } = require("../lib/logger");
const { createMongoConnection } = require("../clients/mongo");
const { createRedisConnection } = require("../clients/redis");
const { createKafkaClient } = require("../clients/kafka");
const { createS3Client } = require("../clients/s3");

const { createTasksHandler } = require("./handlers/tasks");
const { createPathsHandler } = require("./handlers/paths");
const { createWaypointsHandler } = require("./handlers/waypoints");
const { createMapHandler } = require("./handlers/map");

const { createApiApp } = require("./app");

async function startApiCore() {
  const logger = createLogger("api-core");
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

  const handlers = {
    tasks: createTasksHandler({
      collection: mongo.collection(env.mongo.collections.tasks),
      kafkaProducer,
    }),
    paths: createPathsHandler({
      collection: mongo.collection(env.mongo.collections.paths),
    }),
    waypoints: createWaypointsHandler({
      collection: mongo.collection(env.mongo.collections.waypoints),
    }),
    map: createMapHandler({
      redisClient: redis.client,
      s3,
      bucketName: env.aws.bucketName,
      key: env.aws.mapName,
      logger: logger.child("map"),
    }),
  };

  let ready = true;
  const app = createApiApp({
    envConfig: env,
    logger,
    readinessProbe: () => ready,
    handlers,
  });

  const server = app.listen(env.ports.api, () => {
    logger.info(`API core listening on ${env.ports.api}`);
  });

  async function shutdown(signal) {
    logger.info(`Received ${signal}, shutting down API core`);
    ready = false;

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
  }

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
}

startApiCore().catch((error) => {
  console.error(error);
  process.exit(1);
});
