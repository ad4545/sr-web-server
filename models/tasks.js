const { env } = require("../config/env");
const { state } = require("../state/runtimeState");

const getCollection = () => {
  if (!state.api.mongo) {
    throw new Error("MongoDB connection is not initialized.");
  }

  return state.api.mongo.collection(env.mongo.collections.tasks);
};

const saveTask = async (task) => {
  const result = await getCollection().insertOne(task);
  return result.insertedId;
};

const listAll = async () => {
  return getCollection().find({}).toArray();
};

const count = async () => {
  return getCollection().countDocuments();
};

const listPage = async ({ skip, limit }) => {
  return getCollection().find({}).skip(skip).limit(limit).toArray();
};

const deleteByMasterTaskName = async (masterTaskName) => {
  return getCollection().deleteOne({
    masterTaskName,
  });
};

const crypto = require("crypto");

const createTask = async (redisClient, task) => {
  console.log(`[createTask] Called for masterTaskName: "${task.masterTaskName}"`);
  const taskId = crypto.randomUUID();

  const hashData = {
    taskId,
    masterTaskName: task.masterTaskName,
    tasks: task.tasks,
    status: 'queued',
    createdAt: new Date().toISOString(),
    ...(task.topic !== undefined && task.topic !== null ? { topic: task.topic } : {}),
  };

  await redisClient.hset("redis-tasks", taskId, JSON.stringify(hashData));
  await redisClient.zadd("tasks:queue", Date.now(), taskId);

  console.log(`[createTask] Task stored in Hash and queued. ID: "${taskId}"`);
  return hashData;
};

const getAllForwardTasks = async (redisClient) => {
  const taskIds = await redisClient.zrange("tasks:queue", 0, -1);

  if (!taskIds || taskIds.length === 0) {
    return [];
  }

  const tasks = await Promise.all(
    taskIds.map(async (taskId) => {
      const taskJson = await redisClient.hget("redis-tasks", taskId);
      if (!taskJson) return null;

      try {
        return JSON.parse(taskJson);
      } catch (error) {
        return null;
      }
    })
  );

  return tasks.filter(Boolean);
};

const updateSchedule = async (redisClient, taskIds) => {
  const pipeline = redisClient.pipeline();
  taskIds.forEach((taskId, index) => {
    const score = (index + 1) * 1000;
    pipeline.zadd("tasks:queue", score, taskId);
  });
  await pipeline.exec();
};

const getNextTask = async (redisClient) => {
  console.log("[getNextTask] Checking tasks:queue for the next task...");
  const taskIds = await redisClient.zrange("tasks:queue", 0, 0);
  if (!taskIds || taskIds.length === 0) {
    console.log("[getNextTask] No task found in queue.");
    return null;
  }
  const taskId = taskIds[0];
  console.log(`[getNextTask] Found next task ID: "${taskId}". Fetching details...`);

  const taskJson = await redisClient.hget("redis-tasks", taskId);
  if (!taskJson) {
    console.log(`[getNextTask] Details not found in redis-tasks for ID: "${taskId}". Orphaned task queue item! Cleaning it up using ZREM...`);
    await redisClient.zrem("tasks:queue", taskId);
    console.log(`[getNextTask] Orphaned task ID "${taskId}" removed from tasks:queue. Retrying...`);
    return getNextTask(redisClient);
  }

  try {
    const details = JSON.parse(taskJson);
    console.log(`[getNextTask] Successfully retrieved details for ID: "${taskId}"`);
    return {
      taskId,
      details,
    };
  } catch (error) {
    console.error(`[getNextTask] Error parsing JSON for ID: "${taskId}":`, error.message);
    return {
      taskId,
      details: null,
    };
  }
};

const markTaskInProgress = async (redisClient, taskId) => {
  console.log(`[markTaskInProgress] Marking task "${taskId}" as in-progress...`);
  const taskJson = await redisClient.hget("redis-tasks", taskId);
  if (!taskJson) {
    throw new Error(`Task with ID ${taskId} not found in redis-tasks`);
  }

  let taskData;
  try {
    taskData = JSON.parse(taskJson);
  } catch (error) {
    throw new Error(`Failed to parse task JSON for ID ${taskId}: ${error.message}`);
  }

  taskData.status = "in-progress";

  const pipeline = redisClient.pipeline();
  pipeline.hset("redis-tasks", taskId, JSON.stringify(taskData));
  pipeline.zrem("tasks:queue", taskId);
  pipeline.set("tasks:inprogress", taskId);

  await pipeline.exec();
  console.log(`[markTaskInProgress] Task "${taskId}" successfully moved to tasks:inprogress`);

  return taskData;
};

const markTaskCompleted = async (redisClient, taskId) => {
  console.log(`[markTaskCompleted] Marking task "${taskId}" as completed...`);
  const taskJson = await redisClient.hget("redis-tasks", taskId);
  if (!taskJson) {
    throw new Error(`Task with ID ${taskId} not found in redis-tasks`);
  }

  let taskData;
  try {
    taskData = JSON.parse(taskJson);
  } catch (error) {
    throw new Error(`Failed to parse task JSON for ID ${taskId}: ${error.message}`);
  }

  taskData.status = "completed";

  const pipeline = redisClient.pipeline();
  pipeline.hset("redis-tasks", taskId, JSON.stringify(taskData));
  pipeline.del("tasks:inprogress");
  pipeline.zadd("tasks:completed", Date.now(), taskId);

  await pipeline.exec();
  console.log(`[markTaskCompleted] Task "${taskId}" successfully moved to tasks:completed.`);

  // Trigger dispatch at the end of markTaskCompleted flow
  try {
    const { dispatch } = require("../dispatcher");
    console.log("[markTaskCompleted] Triggering dispatch cycle after marking task completed...");
    dispatch().catch((err) => {
      console.error(`[markTaskCompleted] Error during automatic dispatch: ${err.message}`);
    });
  } catch (error) {
    // Avoid crashing if dispatcher is not ready
    console.error(`[markTaskCompleted] Failed to import/run dispatcher: ${error.message}`);
  }

  return taskData;
};

const isRobotBusy = async (redisClient) => {
  console.log("[isRobotBusy] Checking if a task is currently executing (tasks:inprogress)...");
  const exists = await redisClient.exists("tasks:inprogress");
  const busy = exists === 1;
  console.log(`[isRobotBusy] Result: ${busy}`);
  return busy;
};

module.exports = {
  count,
  deleteByMasterTaskName,
  listAll,
  listPage,
  saveTask,
  createTask,
  getAllForwardTasks,
  updateSchedule,
  getNextTask,
  markTaskInProgress,
  markTaskCompleted,
  isRobotBusy,
};
