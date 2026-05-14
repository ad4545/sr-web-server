const { parseBoolean, parseCsv, parseNumber } = require("./env");
const { buildRobotTopics } = require("./grpc-utils");

function readRequiredCsv(value, envName) {
  const parsed = parseCsv(value, "");

  if (!parsed.length) {
    throw new Error(`${envName} is required`);
  }

  return parsed;
}

const robots = readRequiredCsv(process.env.GRPC_ROBOTS, "GRPC_ROBOTS");
const topicSuffixes = Object.freeze({
  odom: readRequiredCsv(process.env.GRPC_ODOM_TOPIC_SUFFIXES, "GRPC_ODOM_TOPIC_SUFFIXES"),
  taskFeedback: readRequiredCsv(
    process.env.GRPC_TASK_FEEDBACK_TOPIC_SUFFIXES,
    "GRPC_TASK_FEEDBACK_TOPIC_SUFFIXES"
  ),
  battery: readRequiredCsv(process.env.GRPC_BATTERY_TOPIC_SUFFIXES, "GRPC_BATTERY_TOPIC_SUFFIXES"),
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
  },
});

module.exports = {
  grpc,
  buildRobotTopics,
};
