const { PROTOBUF_TO_OBJECT_OPTIONS } = require("../../clients/protobuf");
const { createGrpcTopicStream } = require("../grpc/client");

function createTaskFeedbackStream({ grpcConfig, grpcTypes, feedbackType, logger, emitTaskFeedback }) {
  return createGrpcTopicStream({
    grpcConfig,
    logger: logger.child("grpc"),
    label: "task-feedback stream",
    topics: grpcConfig.topics.taskFeedback,
    topicStreamRequestType: grpcTypes.topicStreamRequestType,
    rawDataChunkType: grpcTypes.rawDataChunkType,
    onChunk({ payload }) {
      try {
        const decoded = feedbackType.decode(payload);
        const taskFeedback = feedbackType.toObject(decoded, PROTOBUF_TO_OBJECT_OPTIONS);
        emitTaskFeedback(taskFeedback);
      } catch (error) {
        logger.error("task-feedback stream processing failed", error.message);
      }
    },
  });
}

module.exports = {
  createTaskFeedbackStream,
};
