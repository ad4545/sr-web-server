const http = require('http');
const express = require("express");
const app = express();
const cors = require("cors");
const { MongoClient } = require('mongodb');
const { Kafka } = require('kafkajs');
const { Server } = require('socket.io');
const { redis_client } = require("./utils/redis_client");
const getMap = require("./controller/map.js")

const PORT = 3002;
const MONGO_URI = 'mongodb://10.0.0.6:27017/sevenhub';
const DB_NAME = 'sevenhub';
const COLLECTION_NAME = 'paths';
const TASK_COLLECTION = 'tasks';

let db;

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

// ── Kafka Producer (existing — sends tasks to robot) ──────────────────────
const producerkafka = new Kafka({
  clientId: 'my-app',
  brokers: ['10.0.0.12:9092'], // Replace with your Kafka broker address
});

const producer = producerkafka.producer();

async function connectProducer() {
  await producer.connect();
  console.log('✅ Connected to Kafka');
}

// ── Kafka Consumer (new — listens to robot feedback) ──────────────────────
const consumerKafka = new Kafka({
  clientId: 'robot-feedback-consumer',
  brokers: ['10.0.0.12:9092'],  // 👈 replace if feedback comes from a different broker
});

const consumer = consumerKafka.consumer({ groupId: 'robot-feedback-group' });
let isKafkaConsumerConnected = false;

async function connectConsumer() {
  try {
    await consumer.connect();
    isKafkaConsumerConnected = true;
    console.log('✅ Kafka Consumer connected');
  } catch (err) {
    isKafkaConsumerConnected = false;
    console.error('❌ Kafka Consumer connection failed:', err.message);
  }
}

async function subscribeAndConsume() {
  // Subscribe to robot position updates (continuous stream)
  await consumer.subscribe({
    topic: 'robot-position',
    fromBeginning: false,
  });

  // Subscribe to robot-feedback topic (published when robot starts executing a task)
  await consumer.subscribe({
    topic: 'robot-feedback',
    fromBeginning: false,
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const key   = message.key?.toString();
        const value = JSON.parse(message.value.toString());

        if (topic === 'robot-position') {
          // Continuous position updates
          await processRobotFeedback(key, value);
        } else if (topic === 'robot-feedback') {
          // Published when the robot starts executing a task
          await processTaskExecutionFeedback(key, value);
        } else {
          console.warn(`⚠️  Received message from unhandled topic: ${topic}`);
        }
      } catch (err) {
        console.error(`❌ Error processing message from topic "${topic}":`, err.message);
      }
    },
  });
}

async function processRobotFeedback(key, feedback) {
  // console.log(`⚙️  Processing feedback — Key: "${key}" | Type: "${feedback.type}"`);

  // Forward the raw message to all connected Socket.IO clients on the 'robot-position' channel
  io?.emit('robot-position', { key, ...feedback });

  switch (feedback.type) {

    // ── Fleet Events ──────────────────────────────────────────────────────────

    case 'FLEET_SNAPSHOT':
      // Full fleet state sent when a client first connects
      console.log(`🌐 Fleet Snapshot Received`);
      console.log(`   Total Robots : ${feedback.fleet?.length}`);
      feedback.fleet?.forEach(robot => {
        console.log(`   Robot : ${robot.id} | ${robot.name} | ${robot.status} | Battery: ${robot.battery}%`);
      });
      // e.g. sync full fleet to MongoDB
      // await db.collection('robots').deleteMany({});
      // await db.collection('robots').insertMany(feedback.fleet);
      break;

    case 'ROBOT_ADDED':
      // A new robot has joined the fleet
      console.log(`🤖 Robot Added`);
      console.log(`   ID       : ${feedback.robot?.id}`);
      console.log(`   Name     : ${feedback.robot?.name}`);
      console.log(`   Battery  : ${feedback.robot?.battery}%`);
      console.log(`   Speed    : ${feedback.robot?.speed}`);
      console.log(`   Position : x=${feedback.robot?.position?.x}, y=${feedback.robot?.position?.y}`);
      // e.g. await db.collection('robots').insertOne({ ...feedback.robot, createdAt: new Date() });
      break;

    case 'ROBOT_REMOVED':
      // A robot has been removed from the fleet
      console.log(`🗑️  Robot Removed`);
      console.log(`   Robot ID : ${feedback.robotId}`);
      // e.g. await db.collection('robots').updateOne(
      //   { id: feedback.robotId },
      //   { $set: { removed: true, removedAt: new Date() } }
      // );
      break;

    // ── Robot State Events ────────────────────────────────────────────────────

    case 'ROBOT_STATE':
      // Emitted every tick for every robot — high frequency
      // console.log(`📡 Robot State`);
      // console.log(`   ID         : ${feedback.robotId}`);
      // console.log(`   Status     : ${feedback.status}`);
      // console.log(`   Battery    : ${feedback.battery}%`);
      // console.log(`   Position   : x=${feedback.position?.x}, y=${feedback.position?.y}`);
      // console.log(`   Queue Len  : ${feedback.queueLength}`);
      // console.log(`   Tasks Done : ${feedback.totalTasksDone}`);
      if (feedback.task) {
        console.log(`   Task ID    : ${feedback.task.id}`);
        console.log(`   Task Type  : ${feedback.task.type}`);
        console.log(`   Progress   : ${feedback.task.progress}%`);
        console.log(`   Task Status: ${feedback.task.status}`);
      }
      // ⚠️ This fires every 500ms per robot — avoid heavy DB writes here
      // e.g. update an in-memory cache or use a debounced write
      break;

    case 'ROBOT_STATUS':
      // Robot status changed (e.g. started charging, finished charging)
      console.log(`🔄 Robot Status Changed`);
      console.log(`   Robot ID : ${feedback.robotId}`);
      console.log(`   Status   : ${feedback.status}`);
      if (feedback.reason) {
        console.log(`   Reason   : ${feedback.reason}`); // e.g. 'LOW_BATTERY'
      }
      // e.g. await db.collection('robots').updateOne(
      //   { id: feedback.robotId },
      //   { $set: { status: feedback.status } }
      // );
      break;

    case 'ROBOT_ERROR':
      // A robot encountered a fault mid-task
      console.error(`🚨 Robot Error`);
      console.error(`   Robot ID : ${feedback.robotId}`);
      console.error(`   Task ID  : ${feedback.taskId}`);
      console.error(`   Message  : ${feedback.message}`); // e.g. 'Obstacle detected'
      // e.g. await db.collection('errors').insertOne({
      //   robotId: feedback.robotId,
      //   taskId: feedback.taskId,
      //   message: feedback.message,
      //   createdAt: new Date()
      // });
      break;

    // ── Task Events ───────────────────────────────────────────────────────────

    case 'TASK_QUEUED':
      // A task has been assigned to a robot and added to its queue
      console.log(`📋 Task Queued`);
      console.log(`   Robot ID  : ${feedback.robotId}`);
      console.log(`   Task ID   : ${feedback.task?.id}`);
      console.log(`   Task Type : ${feedback.task?.type}`);
      console.log(`   Priority  : ${feedback.task?.priority}`);
      console.log(`   Status    : ${feedback.task?.status}`);
      console.log(`   Target    : x=${feedback.task?.target?.x}, y=${feedback.task?.target?.y}`);
      break;

    case 'TASK_PENDING':
      // No robot was available — task is waiting in the global pending queue
      console.log(`⏳ Task Pending`);
      console.log(`   Task ID      : ${feedback.task?.id}`);
      console.log(`   Task Type    : ${feedback.task?.type}`);
      console.log(`   Priority     : ${feedback.task?.priority}`);
      console.log(`   Queue Length : ${feedback.queueLength}`);
      console.log(`   Target       : x=${feedback.task?.target?.x}, y=${feedback.task?.target?.y}`);
      break;

    case 'PENDING_QUEUE_UPDATED':
      // A pending task was dispatched to a newly idle robot
      console.log(`🔄 Pending Queue Updated`);
      console.log(`   Queue Length : ${feedback.queueLength}`);
      break;

    case 'TASK_STARTED':
      // A robot picked up a task from its queue and started moving
      console.log(`🚀 Task Started`);
      console.log(`   Robot ID  : ${feedback.robotId}`);
      console.log(`   Task ID   : ${feedback.task?.id}`);
      console.log(`   Task Type : ${feedback.task?.type}`);
      console.log(`   Priority  : ${feedback.task?.priority}`);
      console.log(`   Target    : x=${feedback.task?.target?.x}, y=${feedback.task?.target?.y}`);
      // e.g. await db.collection(TASK_COLLECTION).updateOne(
      //   { taskId: feedback.task.id },
      //   { $set: { status: 'IN_PROGRESS', startedAt: new Date() } }
      // );
      break;

    case 'TASK_COMPLETED':
      // A robot finished a task successfully
      console.log(`✅ Task Completed`);
      console.log(`   Robot ID  : ${feedback.robotId}`);
      console.log(`   Task ID   : ${feedback.task?.id}`);
      console.log(`   Task Type : ${feedback.task?.type}`);
      console.log(`   Progress  : ${feedback.task?.progress}%`);
      // e.g. await db.collection(TASK_COLLECTION).updateOne(
      //   { taskId: feedback.task.id },
      //   { $set: { status: 'COMPLETED', completedAt: new Date() } }
      // );
      break;

    case 'TASK_FAILED':
      // A robot failed a task due to an injected error
      console.error(`❌ Task Failed`);
      console.error(`   Robot ID  : ${feedback.robotId}`);
      console.error(`   Task ID   : ${feedback.task?.id}`);
      console.error(`   Task Type : ${feedback.task?.type}`);
      console.error(`   Progress  : ${feedback.task?.progress}%`);
      // e.g. await db.collection(TASK_COLLECTION).updateOne(
      //   { taskId: feedback.task.id },
      //   { $set: { status: 'FAILED', completedAt: new Date() } }
      // );
      break;

    default:
      console.log(`❓ Unknown feedback type "${feedback.type}":`, feedback);
  }
}

// ── Task Execution Feedback Handler ───────────────────────────────────────
// Called when a message arrives on the 'robot-feedback' topic,
// i.e. when the robot starts executing a task.
async function processTaskExecutionFeedback(key, feedback) {
  console.log(`🤖 Task Execution Feedback — Key: "${key}" | Type: "${feedback.type}"`);

  // Forward the raw message to all connected Socket.IO clients on the 'robot-feedback' channel
  io?.emit('robot-feedback', { key, ...feedback });

  switch (feedback.type) {

    case 'TASK_EXECUTION_STARTED':
      // Robot has begun executing an assigned task
      console.log(`🚀 Task Execution Started`);
      console.log(`   Robot ID  : ${feedback.robotId}`);
      console.log(`   Task ID   : ${feedback.taskId}`);
      console.log(`   Task Name : ${feedback.taskName}`);
      console.log(`   Task Type : ${feedback.taskType}`);
      console.log(`   Started At: ${feedback.startedAt}`);
      // e.g. update DB to mark the task as IN_PROGRESS
      // await db.collection(TASK_COLLECTION).updateOne(
      //   { taskId: feedback.taskId },
      //   { $set: { status: 'IN_PROGRESS', startedAt: new Date(feedback.startedAt) } }
      // );
      break;

    case 'TASK_EXECUTION_PROGRESS':
      // Optional: progress updates published mid-execution
      console.log(`📊 Task Execution Progress`);
      console.log(`   Robot ID  : ${feedback.robotId}`);
      console.log(`   Task ID   : ${feedback.taskId}`);
      console.log(`   Progress  : ${feedback.progress}%`);
      break;

    case 'TASK_EXECUTION_COMPLETED':
      // Robot finished executing the task
      console.log(`✅ Task Execution Completed`);
      console.log(`   Robot ID     : ${feedback.robotId}`);
      console.log(`   Task ID      : ${feedback.taskId}`);
      console.log(`   Completed At : ${feedback.completedAt}`);
      // e.g. await db.collection(TASK_COLLECTION).updateOne(
      //   { taskId: feedback.taskId },
      //   { $set: { status: 'COMPLETED', completedAt: new Date(feedback.completedAt) } }
      // );
      break;

    case 'TASK_EXECUTION_FAILED':
      // Task execution failed on the robot side
      console.error(`❌ Task Execution Failed`);
      console.error(`   Robot ID : ${feedback.robotId}`);
      console.error(`   Task ID  : ${feedback.taskId}`);
      console.error(`   Reason   : ${feedback.reason}`);
      // e.g. await db.collection(TASK_COLLECTION).updateOne(
      //   { taskId: feedback.taskId },
      //   { $set: { status: 'FAILED', failedAt: new Date() } }
      // );
      break;

    default:
      console.log(`❓ Unknown task execution feedback type "${feedback.type}":`, feedback);
  }
}

// connectKafka()

app.use(express.json());
app.use(cors({ origin: "*" }));

// Health check route
app.get("/", async (req, res) => {
  res.send(`Server is running at ${PORT}`);
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

// Save task route
app.post("/save-task", async (req, res) => {
  try {
    const { masterTaskName, tasks, topic } = req.body; // ✅ directly from req.body

    if (!masterTaskName || typeof masterTaskName !== 'string') {
      return res.status(400).json({ error: 'masterTaskName is required and must be a string.' });
    }

    if (!Array.isArray(tasks) || tasks.length === 0) {
      return res.status(400).json({ error: 'tasks is required and must be a non-empty array.' });
    }

    // Validate each task
    for (const task of tasks) {
      if (!task.taskName || typeof task.taskName !== 'string') {
        return res.status(400).json({ error: 'Each task must have a valid taskName.' });
      }

      if (!task.type || typeof task.type !== 'string') {
        return res.status(400).json({ error: 'Each task must have a valid type.' });
      }

      // Validate path if present
      if (task.path && Object.keys(task.path).length > 0) {
        const { pathName, paths } = task.path;

        if (!pathName || typeof pathName !== 'string') {
          return res.status(400).json({ error: 'path.pathName is required and must be a string.' });
        }

        if (!Array.isArray(paths) || paths.length === 0) {
          return res.status(400).json({ error: 'path.paths is required and must be a non-empty array.' });
        }

        for (const point of paths) {
          if (!point.translation || !point.rotation) {
            return res.status(400).json({ error: 'Each path point must have translation and rotation.' });
          }
        }
      }
    }

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
const limit = 2; // 2 tasks per page
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
  console.log('send task called', req.body)
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
      topic: 'temp-task',   // ✅ use dynamic topic from request instead of hardcoded "task"
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

// ── Graceful Shutdown ──────────────────────────────────────────────────────
async function shutdown() {
  console.log('🔌 Shutting down...');
  await producer.disconnect();
  if (isKafkaConsumerConnected) await consumer.disconnect();
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

// ── Bootstrap ──────────────────────────────────────────────────────────────
const httpServer = http.createServer(app);

// ── Socket.IO Server ───────────────────────────────────────────────────────
const io = new Server(httpServer, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST'],
  },
});

io.on('connection', (socket) => {
  console.log(`🔌 New client connected: ${socket.id}`);
  // Clients can listen on:
  //   socket.on('robot-position', (data) => { ... })   ← continuous position stream
  //   socket.on('robot-feedback', (data) => { ... })   ← task execution events
  socket.on('disconnect', () => {
    console.log(`🔌 Client disconnected: ${socket.id}`);
  });
});

connectDB().then(async () => {
  httpServer.listen(PORT, () => {
    console.log(`Server is running at:${PORT}`);
    console.log('✅ WebSocket Server initialized');
  });

  // Start producer
  await connectProducer();

  // Start consumer
  await connectConsumer();
  if (isKafkaConsumerConnected) {
    await subscribeAndConsume();
  }
});
