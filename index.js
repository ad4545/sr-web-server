const http = require('http');
const express = require("express");
const app = express();
const cors = require("cors");
const { MongoClient } = require('mongodb');
const { Kafka } = require('kafkajs');
const { Server } = require("socket.io");
const { redis_client } = require("./utils/redis_client");
const getMap = require("./controller/map.js")

const PORT = 3001;
const MONGO_URI = 'mongodb://10.0.0.6:27017/sevenhub';
const DB_NAME = 'sevenhub';
const COLLECTION_NAME = 'paths';
const TASK_COLLECTION = 'tasks';
const WAYPOINT_COLLECTION = 'waypoints';

const path = require("path");
const protobuf = require("protobufjs");

let odomType;
let feedbackType;

async function loadProto() {
  const root = await protobuf.load(path.join(__dirname, "Odometry.proto"));
  odomType = root.lookupType("combined_odom.Odometry");
}

async function loadFeedbackProto() {
  const root = await protobuf.load(path.join(__dirname, "protobufs", "Feedback.proto"));
  feedbackType = root.lookupType("std_msgs.String");
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
          const payload = odomType.toObject(decoded, {
            longs: String,
            enums: String,
            bytes: String,
            defaults: true,
            arrays: true,
            objects: true,
          });

          // Extract robot ID from topic name, e.g. "amr.002.odom_with_amcl" -> "amr.002"
          const robotId = topic.split(".odom_with_amcl")[0];

          const positionPayload = { robotId, ...payload };

          console.log(positionPayload);

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
          const payload = feedbackType.toObject(decoded, {
            longs: String,
            enums: String,
            bytes: String,
            defaults: true,
            arrays: true,
            objects: true,
          });

          console.log("task feedback ::", payload);

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

connectKafka()

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

    // if (!masterTaskName || typeof masterTaskName !== 'string') {
    //   return res.status(400).json({ error: 'masterTaskName is required and must be a string.' });
    // }

    // if (!Array.isArray(tasks) || tasks.length === 0) {
    //   return res.status(400).json({ error: 'tasks is required and must be a non-empty array.' });
    // }

    // // Validate each task
    // for (const task of tasks) {
    //   if (!task.taskName || typeof task.taskName !== 'string') {
    //     return res.status(400).json({ error: 'Each task must have a valid taskName.' });
    //   }

    //   if (!task.type || typeof task.type !== 'string') {
    //     return res.status(400).json({ error: 'Each task must have a valid type.' });
    //   }

    //   // Validate path if present
    //   if (task.path && Object.keys(task.path).length > 0) {
    //     const { pathName, paths } = task.path;

    //     if (!pathName || typeof pathName !== 'string') {
    //       return res.status(400).json({ error: 'path.pathName is required and must be a string.' });
    //     }

    //     if (!Array.isArray(paths) || paths.length === 0) {
    //       return res.status(400).json({ error: 'path.paths is required and must be a non-empty array.' });
    //     }

    //     for (const point of paths) {
    //       if (!point.translation || !point.rotation) {
    //         return res.status(400).json({ error: 'Each path point must have translation and rotation.' });
    //       }
    //     }
    //   }
    // }

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

    // if (!masterTaskName || typeof masterTaskName !== 'string') {
    //   return res.status(400).json({ error: 'masterTaskName is required and must be a string.' });
    // }

    // if (!Array.isArray(tasks) || tasks.length === 0) {
    //   return res.status(400).json({ error: 'tasks is required and must be a non-empty array.' });
    // }

    // if (!topic || typeof topic !== 'string') {
    //   return res.status(400).json({ error: 'topic is required and must be a string.' });
    // }

    // Publish to Kafka
    await producer.send({
      topic: req.body.task.topic == 'task_sr1' ? 'task' : 'task-2',   // ✅ use dynamic topic from request instead of hardcoded "task"
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
  socket.on("disconnect", () => {
    console.log(`🔌 Client disconnected: ${socket.id}`);
  });
});

connectDB().then(() => {
  httpServer.listen(PORT, () => {
    console.log(`Server is running at:${PORT}`);
    console.log("✅ WebSocket Server initialized");
  });
});
