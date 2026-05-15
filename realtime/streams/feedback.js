const { createGrpcSubscriptionClient } = require("../grpc/client");

function createTaskFeedbackStream({ grpcConfig, grpcTypes, schema, logger, emitTaskFeedback }) {
  return createGrpcSubscriptionClient({
    grpcConfig,
    logger: logger.child("grpc"),
    label: "task-feedback stream",
    subscriptions: grpcConfig.topics.taskFeedback.map((topic) => ({
      topic,
      schema,
    })),
    topicStreamRequestType: grpcTypes.topicStreamRequestType,
    rawDataChunkType: grpcTypes.rawDataChunkType,
    onMessage({ decoded }) {
      try {
        emitTaskFeedback(decoded);
      } catch (error) {
        logger.error("task-feedback stream processing failed", error.message);
      }
    },
  });
}

module.exports = {
  createTaskFeedbackStream,
};
