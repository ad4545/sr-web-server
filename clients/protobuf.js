const path = require("path");
const protobuf = require("protobufjs");
const { PROTOBUF_TO_OBJECT_OPTIONS } = require("../config/constants");

const resolveProjectPath = (...segments) => {
  return path.resolve(process.cwd(), ...segments);
};

const loadType = async (filePathSegments, typeName) => {
  const root = await protobuf.load(resolveProjectPath(...filePathSegments));
  return root.lookupType(typeName);
};

const loadProtobufSchema = async ({
  protoPath,
  typeName,
  toObjectOptions = PROTOBUF_TO_OBJECT_OPTIONS,
}) => {
  const root = await protobuf.load(resolveProjectPath(protoPath));
  const type = root.lookupType(typeName);

  return {
    protoPath,
    typeName,
    type,
    toObjectOptions,
  };
};

const loadOdometryType = async () => {
  return loadType(["protobufs", "Odometry.proto"], "combined_odom.Odometry");
};

const loadFeedbackType = async () => {
  return loadType(["protobufs", "Feedback.proto"], "std_msgs.String");
};

const loadBatteryType = async () => {
  return loadType(["protobufs", "Battery.proto"], "Battery");
};

const loadSpeedType = async () => {
  return loadType(["protobufs", "Speed.proto"], "Speed");
};

const loadTwistType = async () => {
  return loadType(["protobufs", "Twist.proto"], "Twist");
};

const loadStreamRouterTypes = async () => {
  const root = await protobuf.load(resolveProjectPath("protobufs", "StreamRouter.proto"));

  return {
    topicStreamRequestType: root.lookupType("com.example.grpc.TopicStreamRequest"),
    rawDataChunkType: root.lookupType("com.example.grpc.RawDataChunk"),
  };
};

module.exports = {
  PROTOBUF_TO_OBJECT_OPTIONS,
  loadBatteryType,
  loadFeedbackType,
  loadOdometryType,
  loadProtobufSchema,
  loadSpeedType,
  loadStreamRouterTypes,
  loadTwistType,
};
