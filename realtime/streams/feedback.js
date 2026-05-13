const { PROTOBUF_TO_OBJECT_OPTIONS } = require("../../clients/protobuf");

function createTaskFeedbackStream({
  kafkaClient,
  kafkaConfig,
  feedbackType,
  logger,
  emitTaskFeedback,
}) {
  const consumer = kafkaClient.createConsumer(kafkaConfig.groupIds.taskFeedback);
  let connected = false;

  return {
    async start() {
      if (!connected) {
        await consumer.connect();
        for (const topic of kafkaConfig.topics.taskFeedback) {
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
            const decoded = feedbackType.decode(message.value);
            const payload = feedbackType.toObject(decoded, PROTOBUF_TO_OBJECT_OPTIONS);
            emitTaskFeedback(payload);
          } catch (error) {
            logger.error("task-feedback stream processing failed", error.message);
          }
        },
      });

      logger.info(`task-feedback stream started for topics: ${kafkaConfig.topics.taskFeedback.join(", ")}`);
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
  createTaskFeedbackStream,
};
