const { resolveRobotIdFromTopic } = require("../../config/grpc-utils");
const { createGrpcSubscriptionClient } = require("../grpc/client");

function createOdomStream({ grpcConfig, grpcTypes, schema, logger, emitPosition }) {
  return createGrpcSubscriptionClient({
    grpcConfig,
    logger: logger.child("grpc"),
    label: "robot-position stream",
    subscriptions: grpcConfig.topics.odom.map((topic) => ({
      topic,
      schema,
    })),
    topicStreamRequestType: grpcTypes.topicStreamRequestType,
    rawDataChunkType: grpcTypes.rawDataChunkType,
    onMessage({ topic, decoded }) {
      try {
        const robotId = resolveRobotIdFromTopic(topic, grpcConfig.topicSuffixes?.odom || []);

        emitPosition({
          robotId,
          ...decoded,
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
