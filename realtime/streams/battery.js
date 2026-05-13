const { PROTOBUF_TO_OBJECT_OPTIONS } = require("../../clients/protobuf");

function createBatteryStream({ kafkaClient, kafkaConfig, batteryType, logger, emitBattery }) {
  const consumer = kafkaClient.createConsumer(kafkaConfig.groupIds.battery);
  let connected = false;

  return {
    async start() {
      if (!connected) {
        await consumer.connect();
        for (const topic of kafkaConfig.topics.battery) {
          await consumer.subscribe({ topic, fromBeginning: false });
        }
        connected = true;
      }

      await consumer.run({
        eachMessage: async ({ message }) => {
          if (!message.value) {
            return;
          }

          try {
            const decoded = batteryType.decode(message.value);
            const payload = batteryType.toObject(decoded, PROTOBUF_TO_OBJECT_OPTIONS);
            emitBattery(payload);
          } catch (error) {
            logger.error("battery stream processing failed", error.message);
          }
        },
      });

      logger.info(`battery stream started for topics: ${kafkaConfig.topics.battery.join(", ")}`);
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
  createBatteryStream,
};
