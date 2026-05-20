const { parseBoolean, parseCsv, parseNumber } = require("./env");
const { buildRobotTopics } = require("./grpcUtils");
const { PROTOBUF_TO_OBJECT_OPTIONS } = require("./constants");

const readRequiredCsv = (value, envName) => {
  const parsed = parseCsv(value, "");

  if (!parsed.length) {
    throw new Error(`${envName} is required`);
  }

  return parsed;
};

const robots = readRequiredCsv(process.env.GRPC_ROBOTS, "GRPC_ROBOTS");
const topicSuffixes = Object.freeze({
  odom: readRequiredCsv(process.env.GRPC_ODOM_TOPIC_SUFFIXES, "GRPC_ODOM_TOPIC_SUFFIXES"),
  taskFeedback: readRequiredCsv(
    process.env.GRPC_TASK_FEEDBACK_TOPIC_SUFFIXES,
    "GRPC_TASK_FEEDBACK_TOPIC_SUFFIXES"
  ),
  battery: readRequiredCsv(process.env.GRPC_BATTERY_TOPIC_SUFFIXES, "GRPC_BATTERY_TOPIC_SUFFIXES"),
  speed: readRequiredCsv(process.env.GRPC_SPEED_TOPIC_SUFFIXES, "GRPC_SPEED_TOPIC_SUFFIXES"),
});

const grpc = Object.freeze({
  host: process.env.GRPC_STREAM_HOST || "10.0.0.3",
  port: parseNumber(process.env.GRPC_STREAM_PORT, 50051),
  useTls: parseBoolean(process.env.GRPC_STREAM_USE_TLS, false),
  reconnectDelayMs: parseNumber(process.env.GRPC_STREAM_RECONNECT_DELAY_MS, 5000),
  robots,
  topicSuffixes,
  topics: {
    odom: buildRobotTopics(robots, topicSuffixes.odom),
    taskFeedback: buildRobotTopics(robots, topicSuffixes.taskFeedback),
    battery: buildRobotTopics(robots, topicSuffixes.battery),
    speed: buildRobotTopics(robots, topicSuffixes.speed),
  },
  schemas: {
    odom: Object.freeze({
      protoPath: "protobufs/Odometry.proto",
      typeName: "combined_odom.Odometry",
      toObjectOptions: PROTOBUF_TO_OBJECT_OPTIONS,
    }),
    taskFeedback: Object.freeze({
      protoPath: "protobufs/Feedback.proto",
      typeName: "std_msgs.String",
      toObjectOptions: PROTOBUF_TO_OBJECT_OPTIONS,
    }),
    battery: Object.freeze({
      protoPath: "protobufs/Battery.proto",
      typeName: "Battery",
      toObjectOptions: PROTOBUF_TO_OBJECT_OPTIONS,
    }),
    speed: Object.freeze({
      protoPath: "protobufs/Speed.proto",
      typeName: "Speed",
      toObjectOptions: PROTOBUF_TO_OBJECT_OPTIONS,
    }),
  },
});

module.exports = {
  grpc,
  buildRobotTopics,
};
