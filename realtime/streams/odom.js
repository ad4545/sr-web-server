const { PROTOBUF_TO_OBJECT_OPTIONS } = require("../../clients/protobuf");

function createOdomStream({ kafkaClient, kafkaConfig, odometryType, logger, emitPosition }) {
  const consumer = kafkaClient.createConsumer(kafkaConfig.groupIds.odom);
  let connected = false;

  return {
    async start() {
      if (!connected) {
        await consumer.connect();
        for (const topic of kafkaConfig.topics.odom) {
          await consumer.subscribe({ topic, fromBeginning: false });
        }
        connected = true;
      }

      await consumer.run({
        eachMessage: async ({ topic, message }) => {
          if (!message.value) {
            return;
          }

          try {
            const decoded = odometryType.decode(message.value);
            const payload = odometryType.toObject(decoded, PROTOBUF_TO_OBJECT_OPTIONS);
            const robotId = topic.split(".odom_with_amcl")[0];

            emitPosition({
              robotId,
              ...payload,
            });
          } catch (error) {
            logger.error("robot-position stream processing failed", error.message);
          }
        },
      });

      logger.info(`robot-position stream started for topics: ${kafkaConfig.topics.odom.join(", ")}`);
    },

    async stop() {
      if (!connected) {
        return;
      }

      connected = false;
      await consumer.disconnect();
    },
  };
}

module.exports = {
  createOdomStream,
};
