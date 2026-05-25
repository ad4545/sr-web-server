const { isRobotBusy, getNextTask, markTaskInProgress } = require("./models/tasks");
const { getActiveRabbitMqClient, createRabbitMqClient } = require("./clients/rabbitmq");
const { getSocketInstance } = require("./realtime/socket");
const { state } = require("./state/runtimeState");
const { createRedisConnection } = require("./clients/redis");
const { env } = require("./config/env");
const { createLogger } = require("./lib/logger");

let localRedisConnection = null;
let localRabbitMqClient = null;

const getRedisClient = () => {
  console.log("[getRedisClient] Retrieving Redis client...");
  if (state.api?.redis?.client) {
    console.log("[getRedisClient] Returning active api Redis client.");
    return state.api.redis.client;
  }
  if (localRedisConnection) {
    console.log("[getRedisClient] Returning local dispatcher Redis client.");
    return localRedisConnection.client;
  }
  console.log("[getRedisClient] Creating new local dispatcher Redis client connection...");
  const logger = createLogger("dispatcher-redis");
  localRedisConnection = createRedisConnection({
    config: env.redis,
    logger,
  });
  return localRedisConnection.client;
};

const rabbitMqPublish = async ({ exchange, routingKey, content, options }) => {
  console.log(`[rabbitMqPublish] Preparing to publish message to exchange: "${exchange}", routingKey: "${routingKey}"`);
  let client = getActiveRabbitMqClient();
  if (!client) {
    console.log("[rabbitMqPublish] Active RabbitMQ client not found, attempting to get/create local client...");
    if (!localRabbitMqClient) {
      const logger = state.api?.logger || createLogger("dispatcher-rabbitmq");
      localRabbitMqClient = createRabbitMqClient({
        config: env.rabbitmq,
        logger: logger.child("rabbitmq"),
      });
      await localRabbitMqClient.connect().catch((error) => {
        logger.error("Failed to connect to RabbitMQ in dispatcher fallback", error.message);
      });
    }
    client = localRabbitMqClient;
  }
  if (client) {
    console.log("[rabbitMqPublish] Invoking publish on RabbitMQ client...");
    await client.publish({ exchange, routingKey, content, options });
    console.log("[rabbitMqPublish] RabbitMQ client publish call completed.");
  } else {
    console.warn("[rabbitMqPublish] No RabbitMQ client available to publish message.");
  }
};

const dispatch = async () => {
  const logger = state.api?.logger || state.realtime?.logger || createLogger("dispatcher");
  console.log("[dispatch] Starting dispatch cycle...");
  try {
    const redisClient = getRedisClient();
    if (!redisClient) {
      console.log("[dispatch] Redis client not available.");
      return;
    }

    const busy = await isRobotBusy(redisClient);
    if (busy) {
      console.log("[dispatch] Robot is busy. Skipping task dispatch.");
      return;
    }

    const nextTaskResult = await getNextTask(redisClient);
    if (!nextTaskResult || !nextTaskResult.taskId) {
      console.log("[dispatch] No tasks found to dispatch.");
      return;
    }

    const { taskId } = nextTaskResult;
    console.log(`[dispatch] Found task to dispatch: "${taskId}". Processing transitions...`);
    const taskData = await markTaskInProgress(redisClient, taskId);

    console.log(`[dispatch] Publishing task "${taskId}" to RabbitMQ...`);
    // Publish the full task object to RabbitMQ
    await rabbitMqPublish({
      exchange: env.rabbitmq.tasksExchange,
      routingKey: env.rabbitmq.tasksRoutingKey,
      content: Buffer.from(JSON.stringify(taskData)),
    });
    console.log(`[dispatch] Published task "${taskId}" to RabbitMQ successfully.`);

    console.log(`[dispatch] Emitting Socket.io event for task "${taskId}"...`);
    // Emit a socket event called task-status-changed with the updated task object
    const socket = getSocketInstance();
    if (socket) {
      socket.emit("task-status-changed", taskData);
      console.log(`[dispatch] Emitted "task-status-changed" for task "${taskId}" successfully.`);
    } else {
      console.log(`[dispatch] Socket.io instance not active. Skipped socket emission for "${taskId}".`);
    }
  } catch (error) {
    console.error(`[dispatch] Error occurred: ${error.message}`);
    logger.error("Error occurred during dispatch cycle", error);
  }
};

// Set up a setInterval at the bottom that calls dispatch every 5 seconds
// setInterval(() => {
//   dispatch().catch(() => { });
// }, 5000);

module.exports = {
  dispatch,
};
