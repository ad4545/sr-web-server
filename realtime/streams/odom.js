const { PROTOBUF_TO_OBJECT_OPTIONS } = require("../../clients/protobuf");
const { resolveRobotIdFromTopic } = require("../../config/grpc-utils");
const { createGrpcTopicStream } = require("../grpc/client");

function createOdomStream({
  grpcConfig,
  grpcTypes,
  odometryType,
  logger,
  emitPosition,
}) {
  return createGrpcTopicStream({
    grpcConfig,
    logger: logger.child("grpc"),
    label: "robot-position stream",
    topics: grpcConfig.topics.odom,
    topicStreamRequestType: grpcTypes.topicStreamRequestType,
    rawDataChunkType: grpcTypes.rawDataChunkType,
    onChunk({ topic, payload }) {
      try {
        const decoded = odometryType.decode(payload);
        const position = odometryType.toObject(decoded, PROTOBUF_TO_OBJECT_OPTIONS);
        const robotId = resolveRobotIdFromTopic(topic, grpcConfig.topicSuffixes?.odom || []);

        emitPosition({
          robotId,
          ...position,
        });
      } catch (error) {
        logger.error("robot-position stream processing failed", error.message);
      }
    },
  });
}

module.exports = {
  createOdomStream,
};
