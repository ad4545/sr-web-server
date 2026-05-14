const path = require("path");
const protobuf = require("protobufjs");
const { PROTOBUF_TO_OBJECT_OPTIONS } = require("../config/constants");

function resolveProjectPath(...segments) {
  return path.resolve(process.cwd(), ...segments);
}

async function loadType(filePathSegments, typeName) {
  const root = await protobuf.load(resolveProjectPath(...filePathSegments));
  return root.lookupType(typeName);
}

async function loadOdometryType() {
  return loadType(["protobufs", "Odometry.proto"], "combined_odom.Odometry");
}

async function loadFeedbackType() {
  return loadType(["protobufs", "Feedback.proto"], "std_msgs.String");
}

async function loadBatteryType() {
  return loadType(["protobufs", "Battery.proto"], "Battery");
}

async function loadTwistType() {
  return loadType(["protobufs", "Twist.proto"], "Twist");
}

async function loadStreamRouterTypes() {
  const root = await protobuf.load(resolveProjectPath("protobufs", "StreamRouter.proto"));

  return {
    topicStreamRequestType: root.lookupType("com.example.grpc.TopicStreamRequest"),
    rawDataChunkType: root.lookupType("com.example.grpc.RawDataChunk"),
  };
}

module.exports = {
  PROTOBUF_TO_OBJECT_OPTIONS,
  loadBatteryType,
  loadFeedbackType,
  loadOdometryType,
  loadStreamRouterTypes,
  loadTwistType,
};
