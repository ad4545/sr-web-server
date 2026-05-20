const parseNumber = (value, fallback) => {
  if (value === undefined || value === null || value === "") {
    return fallback;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};

const parseCsv = (value, fallback) => {
  const source = value || fallback || "";
  return source
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
};

const parseBoolean = (value, fallback = false) => {
  if (value === undefined || value === null || value === "") {
    return fallback;
  }

  return ["1", "true", "yes", "on"].includes(String(value).toLowerCase());
};

const buildRabbitMqUrl = (config) => {
  if (process.env.RABBITMQ_URL) {
    return process.env.RABBITMQ_URL;
  }

  const normalizedVhost = config.vhost || "/";
  const encodedVhost = normalizedVhost === "/" ? "%2F" : encodeURIComponent(normalizedVhost);
  const credentials = config.username && config.password
    ? `${encodeURIComponent(config.username)}:${encodeURIComponent(config.password)}@`
    : "";

  return `amqp://${credentials}${config.host}:${config.port}/${encodedVhost}`;
};

const defaultOdomTopics = [
  "amr.001.odom_with_amcl",
  "amr.002.odom_with_amcl",
  "amr.003.odom_with_amcl",
  "amr.004.odom_with_amcl",
  "amr.005.odom_with_amcl",
].join(",");

const defaultRabbitConfig = {
  exchange: process.env.RABBITMQ_EXCHANGE || "client",
  exchangeType: process.env.RABBITMQ_EXCHANGE_TYPE || "direct",
  queue: process.env.RABBITMQ_QUEUE || "joystick",
  routingKey: process.env.RABBITMQ_ROUTING_KEY || "joystick",
  reconnectDelayMs: parseNumber(process.env.RABBITMQ_RECONNECT_DELAY_MS, 5000),
  host: process.env.RABBITMQ_HOST || "10.0.0.11",
  port: parseNumber(process.env.RABBITMQ_PORT, 5672),
  username: process.env.RABBITMQ_USERNAME || "",
  password: process.env.RABBITMQ_PASSWORD || "",
  vhost: process.env.RABBITMQ_VHOST || "/",
};

const env = {
  nodeEnv: process.env.NODE_ENV || "development",
  ports: {
    public: parseNumber(process.env.PUBLIC_PORT || process.env.PORT, 3001),
    api: parseNumber(process.env.API_CORE_PORT, 3002),
    realtime: parseNumber(process.env.REALTIME_CORE_PORT, 3003),
  },
  mongo: {
    uri: process.env.MONGO_URI || "mongodb://10.0.0.6:27017/sevenhub",
    dbName: process.env.DB_NAME || "sevenhub",
    collections: {
      tasks: process.env.TASK_COLLECTION || "tasks",
      paths: process.env.PATH_COLLECTION || "paths",
      waypoints: process.env.WAYPOINT_COLLECTION || "waypoints",
    },
  },
  redis: {
    host: process.env.REDIS_HOST || "127.0.0.1",
    port: parseNumber(process.env.REDIS_PORT, 6379),
    enabled: parseBoolean(process.env.REDIS_ENABLED, true),
  },
  aws: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    region: process.env.AWS_REGION || "ap-south-2",
    bucketName: process.env.S3_BUCKET,
    mapName: process.env.MAP_NAME,
    taskCompletionBucket: process.env.TASK_COMPLETION_S3_BUCKET,
  },
  athena: {
    database: process.env.ATHENA_DATABASE,
    workgroup: process.env.ATHENA_WORKGROUP,
    resultsBucket: process.env.ATHENA_RESULTS_BUCKET,
    region: process.env.AWS_REGION || "ap-south-2",
  },
  kafka: {
    brokers: parseCsv(process.env.KAFKA_BROKERS || process.env.KAFKA_BROKER, "10.0.0.12:9092"),
    clientId: process.env.KAFKA_CLIENT_ID || "sr-web-server",
    topics: {
      odom: parseCsv(process.env.ODOM_TOPICS, defaultOdomTopics),
      taskFeedback: parseCsv(process.env.TASK_FEEDBACK_TOPICS, "amr.001.task_feedback"),
      battery: parseCsv(
        process.env.BATTERY_TOPICS,
        "amr.001.uavcanRosBridge.uavcan_ros_bridge.Battery"
      ),
    },
    groupIds: {
      odom: process.env.KAFKA_ODOM_GROUP_ID || "odom-socket-group",
      taskFeedback: process.env.KAFKA_TASK_FEEDBACK_GROUP_ID || "feedback-socket-group",
      battery: process.env.KAFKA_BATTERY_GROUP_ID || "battery-socket-group",
    },
  },
  rabbitmq: {
    ...defaultRabbitConfig,
    url: buildRabbitMqUrl(defaultRabbitConfig),
  },
};

module.exports = {
  env,
  parseBoolean,
  parseCsv,
  parseNumber,
};
