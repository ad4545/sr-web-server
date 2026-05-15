const { createGrpcSubscriptionClient } = require("../grpc/client");

function createBatteryStream({ grpcConfig, grpcTypes, schema, logger, emitBattery }) {
  return createGrpcSubscriptionClient({
    grpcConfig,
    logger: logger.child("grpc"),
    label: "battery stream",
    subscriptions: grpcConfig.topics.battery.map((topic) => ({
      topic,
      schema,
    })),
    topicStreamRequestType: grpcTypes.topicStreamRequestType,
    rawDataChunkType: grpcTypes.rawDataChunkType,
    onMessage({ decoded }) {
      try {
        emitBattery(decoded);
      } catch (error) {
        logger.error("battery stream processing failed", error.message);
      }
    },
  });
}

module.exports = {
  createBatteryStream,
};
