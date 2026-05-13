const amqp = require("amqplib");

function createRabbitMqClient({ config, logger }) {
  let connection = null;
  let channel = null;
  let reconnectTimer = null;
  let connectingPromise = null;
  let lastError = null;

  async function connect() {
    if (channel) {
      return channel;
    }

    if (connectingPromise) {
      return connectingPromise;
    }

    connectingPromise = (async () => {
      const rabbitConnection = await amqp.connect(config.url);
      rabbitConnection.connection.stream.setNoDelay(true);
      const rabbitChannel = await rabbitConnection.createChannel();

      await rabbitChannel.assertExchange(config.exchange, config.exchangeType, { durable: true });
      await rabbitChannel.assertQueue(config.queue, { durable: true });
      await rabbitChannel.bindQueue(config.queue, config.exchange, config.routingKey);

      rabbitConnection.on("error", (error) => {
        logger.error("RabbitMQ connection error", error.message);
      });

      rabbitConnection.on("close", () => {
        logger.warn("RabbitMQ connection closed");
        if (connection === rabbitConnection) {
          connection = null;
          channel = null;
          scheduleReconnect();
        }
      });

      rabbitChannel.on("error", (error) => {
        logger.error("RabbitMQ channel error", error.message);
      });

      rabbitChannel.on("close", () => {
        logger.warn("RabbitMQ channel closed");
        if (channel === rabbitChannel) {
          channel = null;
          connection = null;
          scheduleReconnect();
        }
      });

      connection = rabbitConnection;
      channel = rabbitChannel;
      lastError = null;
      logger.info("RabbitMQ connected");
      return channel;
    })();

    try {
      return await connectingPromise;
    } catch (error) {
      lastError = error;
      scheduleReconnect();
      throw error;
    } finally {
      connectingPromise = null;
    }
  }

  function scheduleReconnect() {
    if (reconnectTimer) {
      return;
    }

    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      connect().catch((error) => {
        logger.error("RabbitMQ reconnect failed", error.message);
      });
    }, config.reconnectDelayMs);
  }

  return {
    async connect() {
      return connect();
    },
    isReady() {
      return Boolean(connection && channel);
    },
    getLastError() {
      return lastError;
    },
    async publish({ exchange, routingKey, content, options }) {
      const activeChannel = await connect();
      const accepted = activeChannel.publish(exchange, routingKey, content, options);

      if (!accepted) {
        logger.warn("RabbitMQ publish backpressure detected");
      }
    },
    async close() {
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }

      await Promise.allSettled([
        channel ? channel.close() : Promise.resolve(),
        connection ? connection.close() : Promise.resolve(),
      ]);

      channel = null;
      connection = null;
      lastError = null;
    },
  };
}

module.exports = {
  createRabbitMqClient,
};
