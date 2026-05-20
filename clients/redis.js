const Redis = require("ioredis");

const createRedisConnection = ({ config, logger }) => {
  if (!config.enabled) {
    return {
      client: null,
      async close() {},
    };
  }

  const client = new Redis({
    host: config.host,
    port: config.port,
    retryStrategy(times) {
      return Math.min(times * 50, 2000);
    },
  });

  client.on("error", (error) => {
    logger.error("Redis client error", error.message);
  });

  return {
    client,
    async close() {
      await client.quit();
      logger.info("Redis connection closed");
    },
  };
};

module.exports = {
  createRedisConnection,
};
