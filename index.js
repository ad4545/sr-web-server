const http = require('http');
require("dotenv").config();
const express = require("express");
const app = express();
const cors = require("cors");
const { MongoClient } = require('mongodb');
const { Kafka } = require('kafkajs');
const { Server } = require("socket.io");
const { redis_client } = require("./utils/redis_client");
const getMap = require("./controller/map.js")

const PORT = Number(process.env.PORT) || 3001;
const MONGO_URI = process.env.MONGO_URI || 'mongodb://10.0.0.6:27017/sevenhub';
const DB_NAME = 'sevenhub';
const COLLECTION_NAME = 'paths';
const TASK_COLLECTION = 'tasks';
const WAYPOINT_COLLECTION = 'waypoints';

const path = require("path");
const protobuf = require("protobufjs");

let amqp;
try {
  amqp = require("amqplib");
} catch (error) {
  console.warn("⚠️ amqplib is not installed. Twist RabbitMQ publishing will stay unavailable until dependencies are installed.");
}

let odomType;
let feedbackType;
let twistType;
let batteryType;
let rabbitConnection;
let rabbitChannel;
let rabbitReconnectTimer;
let isRabbitConnected = false;

const PROTOBUF_TO_OBJECT_OPTIONS = Object.freeze({
  longs: String,
  enums: String,
  bytes: String,
  defaults: true,
  arrays: true,
  objects: true,
});

const RABBITMQ_CONFIG = Object.freeze({
  exchange: "client",
  exchangeType: "direct",
  queue: "joystick",
  routingKey: "joystick",
  reconnectDelayMs: 5000,
  host: process.env.RABBITMQ_HOST || "10.0.0.11",
  port: Number(process.env.RABBITMQ_PORT) || 5672,
  username: process.env.RABBITMQ_USERNAME || "",
  password: process.env.RABBITMQ_PASSWORD || "",
  vhost: process.env.RABBITMQ_VHOST || "/",
});

// Joystick messages are real-time commands; avoid broker disk persistence to minimize publish latency.
const TWIST_PUBLISH_OPTIONS = Object.freeze({
  persistent: false,
  deliveryMode: 1,
  contentType: "application/x-protobuf",
  headers: {
    "protobuf-type": "Twist",
  },
});

function buildRabbitMqUrl() {
  if (process.env.RABBITMQ_URL) {
    return process.env.RABBITMQ_URL;
  }

  const normalizedVhost = RABBITMQ_CONFIG.vhost || "/";
  const encodedVhost = normalizedVhost === "/"
    ? "%2F"
    : encodeURIComponent(normalizedVhost);

  if (RABBITMQ_CONFIG.username && RABBITMQ_CONFIG.password) {
    return `amqp://${encodeURIComponent(RABBITMQ_CONFIG.username)}:${encodeURIComponent(RABBITMQ_CONFIG.password)}@${RABBITMQ_CONFIG.host}:${RABBITMQ_CONFIG.port}/${encodedVhost}`;
  }

  return `amqp://${RABBITMQ_CONFIG.host}:${RABBITMQ_CONFIG.port}/${encodedVhost}`;
}

const RABBITMQ_URL = buildRabbitMqUrl();

async function loadProto() {
  const root = await protobuf.load(path.join(__dirname, "Odometry.proto"));
  odomType = root.lookupType("combined_odom.Odometry");
}

async function loadFeedbackProto() {
  const root = await protobuf.load(path.join(__dirname, "protobufs", "Feedback.proto"));
  feedbackType = root.lookupType("std_msgs.String");
}

async function loadTwistProto() {
  const root = await protobuf.load(path.join(__dirname, "protobufs", "Twist.proto"));
  twistType = root.lookupType("Twist");
}

async function loadBatteryProto() {
  const root = await protobuf.load(path.join(__dirname, "protobufs", "Battery.proto"));
  batteryType = root.lookupType("Battery");
}

function safeAck(ack, payload) {
  if (typeof ack === "function") {
    ack(payload);
  }
}

function validateObjectKeys(value, allowedKeys, label) {
  const extraKeys = Object.keys(value).filter((key) => !allowedKeys.includes(key));
  if (extraKeys.length > 0) {
    return `${label} contains unsupported field(s): ${extraKeys.join(", ")}`;
  }

  return null;
}

function validateVector3(value, label) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return `${label} must be an object with X, Y, and Z numeric fields.`;
  }

  const keyError = validateObjectKeys(value, ["X", "Y", "Z"], label);
  if (keyError) {
    return keyError;
  }

  for (const axis of ["X", "Y", "Z"]) {
    if (typeof value[axis] !== "number" || Number.isNaN(value[axis])) {
      return `${label}.${axis} must be a valid number.`;
    }
  }

  return null;
}

function validateTwistPayload(payload) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    return "Twist payload must be an object.";
  }

  const keyError = validateObjectKeys(payload, ["Linear", "Angular"], "Twist payload");
  if (keyError) {
    return keyError;
  }

  for (const field of ["Linear", "Angular"]) {
    if (!(field in payload)) {
      return `${field} is required.`;
    }
  }

  const linearError = validateVector3(payload.Linear, "Linear");
  if (linearError) {
    return linearError;
  }

  const angularError = validateVector3(payload.Angular, "Angular");
  if (angularError) {
    return angularError;
  }

  return twistType.verify(payload);
}

function isRabbitReady() {
  return Boolean(rabbitChannel && isRabbitConnected);
}

function encodeTwistPayload(payload) {
  const twistMessage = twistType.create(payload);
  return twistType.encode(twistMessage).finish();
}

function publishTwistBuffer(buffer) {
  const publishAccepted = rabbitChannel.publish(
    RABBITMQ_CONFIG.exchange,
    RABBITMQ_CONFIG.routingKey,
    buffer,
    TWIST_PUBLISH_OPTIONS
  );

  if (!publishAccepted) {
    console.warn("⚠️ RabbitMQ publish backpressure detected for twist message");
  }
}

function scheduleRabbitReconnect() {
  if (rabbitReconnectTimer) {
    return;
  }

  console.log(`⚠️ RabbitMQ reconnect scheduled in ${RABBITMQ_CONFIG.reconnectDelayMs / 1000}s`);
  rabbitReconnectTimer = setTimeout(() => {
    rabbitReconnectTimer = null;
    connectRabbitMQ().catch((error) => {
      console.error("❌ RabbitMQ reconnect failed:", error.message);
    });
  }, RABBITMQ_CONFIG.reconnectDelayMs);
}

function resetRabbitState() {
  isRabbitConnected = false;
  rabbitChannel = null;
  rabbitConnection = null;
}

function attachRabbitListeners(connection, channel) {
  connection.on("error", (error) => {
    console.error("❌ RabbitMQ connection error:", error.message);
  });

  connection.on("close", () => {
    console.warn("⚠️ RabbitMQ connection closed");
    if (rabbitConnection === connection) {
      resetRabbitState();
      scheduleRabbitReconnect();
    }
  });

  channel.on("error", (error) => {
    console.error("❌ RabbitMQ channel error:", error.message);
  });

  channel.on("close", () => {
    console.warn("⚠️ RabbitMQ channel closed");
    if (rabbitChannel === channel) {
      resetRabbitState();
      scheduleRabbitReconnect();
    }
  });
}

async function connectRabbitMQ() {
  if (!amqp) {
    console.error("❌ RabbitMQ client dependency missing. Install amqplib to enable Twist publishing.");
    return;
  }

  if (isRabbitConnected && rabbitChannel) {
    return;
  }

  try {
    if (!RABBITMQ_CONFIG.username || !RABBITMQ_CONFIG.password) {
      console.warn("⚠️ RabbitMQ credentials are not configured. Remote brokers usually reject the default guest login. Set RABBITMQ_URL or RABBITMQ_USERNAME/RABBITMQ_PASSWORD.");
    }

    const connection = await amqp.connect(RABBITMQ_URL);
    connection.connection.stream.setNoDelay(true);
    const channel = await connection.createChannel();

    await channel.assertExchange(RABBITMQ_CONFIG.exchange, RABBITMQ_CONFIG.exchangeType, { durable: true });
    await channel.assertQueue(RABBITMQ_CONFIG.queue, { durable: true });
    await channel.bindQueue(RABBITMQ_CONFIG.queue, RABBITMQ_CONFIG.exchange, RABBITMQ_CONFIG.routingKey);

    rabbitConnection = connection;
    rabbitChannel = channel;
    isRabbitConnected = true;

    attachRabbitListeners(connection, channel);

    console.log(`✅ RabbitMQ connected at ${RABBITMQ_URL}`);
    console.log(`✅ RabbitMQ topology ready: exchange='${RABBITMQ_CONFIG.exchange}' queue='${RABBITMQ_CONFIG.queue}' routingKey='${RABBITMQ_CONFIG.routingKey}'`);
  } catch (error) {
    resetRabbitState();
    console.error("❌ RabbitMQ connection failed:", error.message);
    if (error.message.includes("ACCESS_REFUSED")) {
      console.error("❌ RabbitMQ rejected the login. Configure valid broker credentials with RABBITMQ_URL or RABBITMQ_USERNAME/RABBITMQ_PASSWORD.");
    }
    if (error.message.includes("Expected ConnectionOpenOk")) {
      console.error(`❌ RabbitMQ rejected vhost '${RABBITMQ_CONFIG.vhost}'. Verify that the vhost exists and that user '${RABBITMQ_CONFIG.username || "guest"}' has access to it.`);
    }
    scheduleRabbitReconnect();
  }
}

let db;
let isKafkaConnected = false; // ✅ track kafka connection status

// Connect to MongoDB
async function connectDB() {
  const client = new MongoClient(MONGO_URI);
  try {
    await client.connect();
    db = client.db(DB_NAME);           // ✅ no 'const', assigns to outer db
    console.log("✅ Connected to MongoDB");
    const collections = await db.listCollections().toArray();
    console.log(collections);
  } catch (err) {
    console.error("❌ Error:", err);
    process.exit(1);
  }
}

// Kafka setup
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['10.0.0.12:9092'], // Replace with your Kafka broker address
});

const producer = kafka.producer();

async function connectKafka() {
  try {
    await producer.connect();
    isKafkaConnected = true;
    console.log('✅ Kafka Producer connected');

    // Initialize Odom Consumer for WebSockets
    await startOdomConsumer();

    // Initialize Feedback Consumer for WebSockets
    await startFeedbackConsumer();

    // Initialize Battery Consumer for WebSockets
    await startBatteryConsumer();
  } catch (err) {
    isKafkaConnected = false;
    console.error('❌ Kafka connection failed:', err.message);
  }
}

const odomConsumer = kafka.consumer({ groupId: 'odom-socket-group' });

const ODOM_TOPICS = [
  "amr.001.odom_with_amcl",
  "amr.002.odom_with_amcl",
  "amr.003.odom_with_amcl",
  "amr.004.odom_with_amcl",
  "amr.005.odom_with_amcl",
];

async function startOdomConsumer() {
  try {
    await loadProto();

    await odomConsumer.connect();

    // Subscribe to all 5 AMR odometry topics
    for (const topic of ODOM_TOPICS) {
      await odomConsumer.subscribe({ topic, fromBeginning: false });
    }

    await odomConsumer.run({
      eachMessage: async ({ topic, message }) => {
        try {
          if (!message.value) return;

          // Decode protobuf Buffer -> Odometry message
          const decoded = odomType.decode(message.value);

          // Convert protobuf message -> plain JSON object
          const payload = odomType.toObject(decoded, PROTOBUF_TO_OBJECT_OPTIONS);

          // Extract robot ID from topic name, e.g. "amr.002.odom_with_amcl" -> "amr.002"
          const robotId = topic.split(".odom_with_amcl")[0];

          const positionPayload = { robotId, ...payload };

          // console.log(positionPayload);

          io?.emit("position", positionPayload);
        } catch (err) {
          console.error("❌ Error decoding/emitting odom message:", err.message);
        }
      },
    });

    console.log(`✅ Kafka Odom Consumer started for topics: ${ODOM_TOPICS.join(", ")}`);
  } catch (err) {
    console.error("❌ Kafka Odom Consumer failed:", err.message);
  }
}

// ── Feedback Consumer ──────────────────────────────────────────────────────
const feedbackConsumer = kafka.consumer({ groupId: 'feedback-socket-group' });
const batteryConsumer = kafka.consumer({ groupId: 'battery-socket-group' });

async function startFeedbackConsumer() {
  try {
    await loadFeedbackProto();

    await feedbackConsumer.connect();
    await feedbackConsumer.subscribe({
      topic: "amr.001.task_feedback",
      fromBeginning: false,
    });

    await feedbackConsumer.run({
      eachMessage: async ({ message }) => {
        try {
          if (!message.value) return;

          // Decode protobuf Buffer -> std_msgs.String message
          const decoded = feedbackType.decode(message.value);

          // Convert protobuf message -> plain JSON object
          const payload = feedbackType.toObject(decoded, PROTOBUF_TO_OBJECT_OPTIONS);

          // console.log("task feedback ::", payload);

          io?.emit("task_feedback", payload);
        } catch (err) {
          console.error("❌ Error decoding/emitting feedback message:", err.message);
        }
      },
    });

    console.log("✅ Kafka Feedback Consumer started");
  } catch (err) {
    console.error("❌ Kafka Feedback Consumer failed:", err.message);
  }
}

// ── Battery Consumer ───────────────────────────────────────────────────────
async function startBatteryConsumer() {
  try {
    await loadBatteryProto();

    await batteryConsumer.connect();
    await batteryConsumer.subscribe({
      topic: "amr.001.uavcanRosBridge.uavcan_ros_bridge.Battery",
      fromBeginning: false,
    });

    await batteryConsumer.run({
      eachMessage: async ({ message }) => {
        try {
          if (!message.value) return;

          const decoded = batteryType.decode(message.value);
          const payload = batteryType.toObject(decoded, PROTOBUF_TO_OBJECT_OPTIONS);

          io?.emit("battery", payload);
        } catch (err) {
          console.error("❌ Error decoding/emitting battery message:", err.message);
        }
      },
    });

    console.log("✅ Kafka Battery Consumer started");
  } catch (err) {
    console.error("❌ Kafka Battery Consumer failed:", err.message);
  }
}

app.use(express.json());
app.use(cors({ origin: "*" }));

// Health check route
app.get("/", async (req, res) => {
  res.send(`Server is running at ${PORT}`);
});

// Save waypoint route
app.post("/save-waypoint", async (req, res) => {
  try {
    const { waypointName, cords, neighbour } = req.body.waypoint || req.body;

    if (!waypointName || typeof waypointName !== 'string') {
      return res.status(400).json({ error: 'waypointName is required and must be a string.' });
    }

    if (!Array.isArray(cords) || cords.length === 0) {
      return res.status(400).json({ error: 'cords is required and must be a non-empty array.' });
    }

    const result = await db.collection(WAYPOINT_COLLECTION).insertOne({
      waypointName,
      cords,
      neighbour: Array.isArray(neighbour) ? neighbour : [], // ✅ inclusion of neighbour array
      createdAt: new Date(),
    });

    // ✅ Mutual Neighbor Addition: Update each neighbor to include this new waypoint as their neighbor
    if (Array.isArray(neighbour) && neighbour.length > 0) {
      await db.collection(WAYPOINT_COLLECTION).updateMany(
        { waypointName: { $in: neighbour } },
        { $addToSet: { neighbour: waypointName } }
      );
    }

    res.status(201).json({
      message: 'Waypoint saved successfully',
      insertedId: result.insertedId,
    });
  } catch (err) {
    res.status(500).json({ error: 'Internal server error', details: err.message });
  }
});

// Save path route
// Save path route
app.post("/save-path", async (req, res) => {
  try {
    const { pathName, paths } = req.body.path; // ✅ extract from req.body.path

    if (!pathName || typeof pathName !== 'string') {
      return res.status(400).json({ error: 'pathName is required and must be a string.' });
    }

    if (!Array.isArray(paths) || paths.length === 0) {
      return res.status(400).json({ error: 'paths is required and must be a non-empty array.' });
    }

    const result = await db.collection(COLLECTION_NAME).insertOne({
      pathName,
      paths,
      createdAt: new Date(),
    });

    res.status(201).json({
      message: 'Path saved successfully',
      insertedId: result.insertedId,
    });
  } catch (err) {
    res.status(500).json({ error: 'Internal server error', details: err.message });
  }
});

app.get("/get-paths", async (req, res) => {
  try {
    const paths = await db.collection(COLLECTION_NAME).find({}).toArray();

    res.status(200).json({
      message: 'Paths fetched successfully',
      count: paths.length,
      data: paths,
    });
  } catch (err) {
    res.status(500).json({ error: 'Internal server error', details: err.message });
  }
});

app.get("/get-all-waypoints", async (req, res) => {
  try {
    const waypoints = await db.collection(WAYPOINT_COLLECTION).find({}).toArray();

    res.status(200).json({
      message: 'Waypoints fetched successfully',
      count: waypoints.length,
      data: waypoints,
    });
  } catch (err) {
    res.status(500).json({ error: 'Internal server error', details: err.message });
  }
});

// Save task route
app.post("/save-task", async (req, res) => {
  console.log("save task called")
  try {
    const { masterTaskName, tasks, topic } = req.body; // ✅ directly from req.body
    console.log("task data :", masterTaskName, tasks, topic)

    const result = await db.collection(TASK_COLLECTION).insertOne({
      masterTaskName,
      tasks,
      topic: topic || null,
      createdAt: new Date(),
    });

    res.status(201).json({
      message: 'Task saved successfully',
      insertedId: result.insertedId,
    });
  } catch (err) {
    res.status(500).json({ error: 'Internal server error', details: err.message });
  }
});

// Get all tasks route
app.get("/get-all-tasks", async (req, res) => {
  try {
    const tasks = await db.collection(TASK_COLLECTION).find({}).toArray();

    if (tasks.length === 0) {
      return res.status(404).json({ message: 'No tasks found.' });
    }

    res.status(200).json({
      message: 'Tasks fetched successfully',
      count: tasks.length,
      data: tasks,
    });
  } catch (err) {
    res.status(500).json({ error: 'Internal server error', details: err.message });
  }
});

app.get("/get-tasks", async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1; // default page 1
    const limit = 3; // 2 tasks per page
    const skip = (page - 1) * limit;

    const totalCount = await db.collection(TASK_COLLECTION).countDocuments();
    const tasks = await db.collection(TASK_COLLECTION)
      .find({})
      .skip(skip)
      .limit(limit)
      .toArray();

    res.status(200).json({
      message: 'Tasks fetched successfully',
      currentPage: page,
      totalPages: Math.ceil(totalCount / limit),
      totalCount,
      hasNextPage: page < Math.ceil(totalCount / limit),
      hasPrevPage: page > 1,
      data: tasks,
    });
  } catch (err) {
    res.status(500).json({ error: 'Internal server error', details: err.message });
  }
});

app.post("/send-task", async (req, res) => {
  console.log('send task called', req.body.task.topic)
  try {
    const { masterTaskName, tasks, topic } = req.body.task; // ✅ unwrap from req.body.task

    // Publish to Kafka
    await producer.send({
      topic: req.body.task.topic,   // ✅ use dynamic topic from request instead of hardcoded "task"
      messages: [
        {
          key: masterTaskName,
          value: JSON.stringify({ masterTaskName, tasks }),
        },
      ],
    });

    res.status(200).json({
      message: `Task published to Kafka topic '${topic}' successfully`,
    });
  } catch (err) {
    res.status(500).json({ error: 'Internal server error', details: err.message });
  }
});

app.delete("/delete-task/:title", async (req, res) => {
  try {
    const { title } = req.params;

    console.log('Delete called with title:', title);

    // Log ALL tasks to verify DB connection and data
    const allTasks = await db.collection(TASK_COLLECTION).find({}).toArray();
    console.log('All masterTaskNames in DB:', allTasks.map(t => t.masterTaskName));

    const result = await db.collection(TASK_COLLECTION).deleteOne({
      masterTaskName: title,
    });

    console.log('deletedCount:', result.deletedCount);

    if (result.deletedCount === 0) {
      return res.status(404).json({ error: `Task '${title}' not found.` });
    }

    res.status(200).json({
      message: `Task '${title}' deleted successfully`,
    });

  } catch (err) {
    console.log('Delete error:', err.message);
    res.status(500).json({ error: 'Internal server error', details: err.message });
  }
});

app.get("/get-map", getMap)

async function handleTwistMessage(payload, ack) {
  try {
    if (!twistType) {
      safeAck(ack, { ok: false, error: "Twist protobuf schema is not loaded." });
      return;
    }

    if (!isRabbitReady()) {
      safeAck(ack, { ok: false, error: "RabbitMQ is not connected." });
      return;
    }

    const validationError = validateTwistPayload(payload);
    if (validationError) {
      console.error("❌ Invalid twist payload:", validationError);
      safeAck(ack, { ok: false, error: validationError });
      return;
    }

    const encodedPayload = encodeTwistPayload(payload);
    publishTwistBuffer(Buffer.from(encodedPayload));
    safeAck(ack, { ok: true });
  } catch (error) {
    console.error("❌ Failed to publish twist message:", error.message);
    safeAck(ack, { ok: false, error: "Failed to publish twist message." });
  }
}

// HTTP Server & Socket.io
const httpServer = http.createServer(app);
const io = new Server(httpServer, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

io.on("connection", (socket) => {
  console.log(`🔌 New client connected: ${socket.id}`);
  socket.on("twist", handleTwistMessage);

  socket.on("disconnect", () => {
    console.log(`🔌 Client disconnected: ${socket.id}`);
  });
});

async function startServer() {
  try {
    await Promise.all([
      connectDB(),
      loadTwistProto(),
    ]);

    connectKafka();
    connectRabbitMQ().catch((error) => {
      console.error("❌ RabbitMQ startup failed:", error.message);
    });

    httpServer.listen(PORT, () => {
      console.log(`Server is running at:${PORT}`);
      console.log("✅ WebSocket Server initialized");
    });
  } catch (error) {
    console.error("❌ Server startup failed:", error.message);
    process.exit(1);
  }
}

startServer();
