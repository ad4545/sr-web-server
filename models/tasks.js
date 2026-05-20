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
  const taskId = crypto.randomUUID();
 
  const hashData = {
    taskId,
    masterTaskName: task.masterTaskName,
    tasks: task.tasks,
    createdAt: new Date().toISOString(),
    ...(task.topic !== undefined && task.topic !== null ? { topic: task.topic } : {}),
  };
 
  await redisClient.hset("redis-tasks", taskId, JSON.stringify(hashData));
  await redisClient.zadd("tasks:queue", Date.now(), taskId);
 
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
 
module.exports = {
  count,
  deleteByMasterTaskName,
  listAll,
  listPage,
  saveTask,
  createTask,
  getAllForwardTasks,
  updateSchedule,
};
