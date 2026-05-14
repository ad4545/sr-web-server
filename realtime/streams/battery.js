const { PROTOBUF_TO_OBJECT_OPTIONS } = require("../../clients/protobuf");
const { createGrpcTopicStream } = require("../grpc/client");

function createBatteryStream({ grpcConfig, grpcTypes, batteryType, logger, emitBattery }) {
  return createGrpcTopicStream({
    grpcConfig,
    logger: logger.child("grpc"),
    label: "battery stream",
    topics: grpcConfig.topics.battery,
    topicStreamRequestType: grpcTypes.topicStreamRequestType,
    rawDataChunkType: grpcTypes.rawDataChunkType,
    onChunk({ payload }) {
      try {
        const decoded = batteryType.decode(payload);
        const battery = batteryType.toObject(decoded, PROTOBUF_TO_OBJECT_OPTIONS);
        emitBattery(battery);
      } catch (error) {
        logger.error("battery stream processing failed", error.message);
      }
    },
  });
}

module.exports = {
  createBatteryStream,
};
