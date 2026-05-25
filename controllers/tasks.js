const crypto = require("crypto");
const { NotFoundError, ValidationError, sendErrorResponse } = require("../lib/errors");
const tasksModel = require("../models/tasks");
const { state } = require("../state/runtimeState");
const { dispatch } = require("../dispatcher");

const saveTask = async (req, res) => {
  const { params } = req;
  const body = req.body;
  const { query } = req;

  try {
    if (!body || typeof body !== "object" || Array.isArray(body)) {
      throw new ValidationError("task payload must be an object.");
    }
    if (typeof body.masterTaskName !== "string" || body.masterTaskName.length === 0) {
      throw new ValidationError("masterTaskName must be a non-empty string.");
    }
    if (!Array.isArray(body.tasks)) {
      throw new ValidationError("tasks must be an array.");
    }
    if (body.topic !== undefined && body.topic !== null && typeof body.topic !== "string") {
      throw new ValidationError("topic must be a string.");
    }

    for (const task of body.tasks) {
      if (!task || typeof task !== "object" || Array.isArray(task)) {
        throw new ValidationError("task must be an object.");
      }
      if (typeof task.taskName !== "string" || task.taskName.length === 0) {
        throw new ValidationError("Each task.taskName must be a non-empty string.");
      }
      if (typeof task.type !== "string" || task.type.length === 0) {
        throw new ValidationError("Each task.type must be a non-empty string.");
      }
      if (task.path !== undefined && task.path !== null) {
        if (!task.path || typeof task.path !== "object" || Array.isArray(task.path)) {
          throw new ValidationError("path must be an object.");
        }
        if (typeof task.path.pathName !== "string" || task.path.pathName.length === 0) {
          throw new ValidationError("path.pathName must be a non-empty string.");
        }
        if (!Array.isArray(task.path.paths)) {
          throw new ValidationError("path.paths must be an array.");
        }
        for (const point of task.path.paths) {
          if (!point || typeof point !== "object" || Array.isArray(point)) {
            throw new ValidationError("path point must be an object.");
          }
          if (!point.translation || !point.rotation) {
            throw new ValidationError("Each path point must have translation and rotation.");
          }
        }
      }
    }

    const insertedId = await tasksModel.saveTask({
      masterTaskName: body.masterTaskName,
      tasks: body.tasks,
      topic: body.topic,
      createdAt: new Date(),
    });
    return res.status(201).json({
      message: "Task saved successfully",
      insertedId,
    });
  } catch (error) {
    return sendErrorResponse(res, error, state.api.logger);
  }
};

const getAllTasks = async (req, res) => {
  const { params } = req;
  const { body } = req;
  const { query } = req;

  try {
    const tasks = await tasksModel.listAll();
    if (tasks.length === 0) {
      return res.status(404).json({ message: "No tasks found." });
    }

    return res.status(200).json({
      message: "Tasks fetched successfully",
      count: tasks.length,
      data: tasks,
    });
  } catch (error) {
    return sendErrorResponse(res, error, state.api.logger);
  }
};

const getTasks = async (req, res) => {
  const { params } = req;
  const { body } = req;
  const query = req.query || {};

  try {
    const currentPage = (() => {
      const page = Number.parseInt(query.page, 10) || 1;
      return Number.isFinite(page) && page > 0 ? page : 1;
    })();
    const totalCount = await tasksModel.count();
    const skip = (currentPage - 1) * 3;
    const tasks = await tasksModel.listPage({ skip, limit: 3 });
    const totalPages = Math.ceil(totalCount / 3);

    return res.status(200).json({
      message: "Tasks fetched successfully",
      currentPage,
      totalPages,
      totalCount,
      hasNextPage: currentPage < totalPages,
      hasPrevPage: currentPage > 1,
      data: tasks,
    });
  } catch (error) {
    return sendErrorResponse(res, error, state.api.logger);
  }
};

const sendTask = async (req, res) => {
  const { params } = req;
  const body = req.body;
  const { query } = req;

  try {
    const task = body?.task;
    if (!task || typeof task !== "object" || Array.isArray(task)) {
      throw new ValidationError("task must be an object.");
    }
    if (typeof task.masterTaskName !== "string" || task.masterTaskName.length === 0) {
      throw new ValidationError("masterTaskName must be a non-empty string.");
    }
    if (!Array.isArray(task.tasks)) {
      throw new ValidationError("tasks must be an array.");
    }
    if (task.topic !== undefined && task.topic !== null && typeof task.topic !== "string") {
      throw new ValidationError("topic must be a string.");
    }

    await state.api.kafkaProducer.send({
      topic: task.topic,
      messages: [
        {
          key: task.masterTaskName,
          value: JSON.stringify({
            masterTaskName: task.masterTaskName,
            tasks: task.tasks,
          }),
        },
      ],
    });

    return res.status(200).json({
      message: `Task published to Kafka topic '${task.topic}' successfully`,
    });
  } catch (error) {
    return sendErrorResponse(res, error, state.api.logger);
  }
};

const forwardTask = async (req, res) => {
  const body = req.body;
 
  try {
    const task = body?.task;
 
    if (!task || typeof task !== "object" || Array.isArray(task)) {
      throw new ValidationError("task must be an object.");
    }
    if (typeof task.masterTaskName !== "string" || task.masterTaskName.length === 0) {
      throw new ValidationError("masterTaskName must be a non-empty string.");
    }
    if (!Array.isArray(task.tasks)) {
      throw new ValidationError("tasks must be an array.");
    }
    if (task.topic !== undefined && task.topic !== null && typeof task.topic !== "string") {
      throw new ValidationError("topic must be a string.");
    }
 
    console.log(`[forwardTask] Received task to forward. masterTaskName: "${task.masterTaskName}", topic: "${task.topic || "N/A"}"`);
 
    if (!state.api.redis || !state.api.redis.client) {
      throw new Error("Redis client is not initialized.");
    }

    if (task.masterTaskName === "Cancel") {
      console.log("[forwardTask] Cancel task received. Deleting active and pending tasks...");

      // 1. Delete all active and pending tasks from Redis
      const activeTaskId = await state.api.redis.client.get("tasks:inprogress");
      const pendingTaskIds = await state.api.redis.client.zrange("tasks:queue", 0, -1);
      const allTaskIdsToDelete = [];
      if (activeTaskId) allTaskIdsToDelete.push(activeTaskId);
      if (pendingTaskIds && pendingTaskIds.length > 0) {
        allTaskIdsToDelete.push(...pendingTaskIds);
      }

      const pipeline = state.api.redis.client.pipeline();
      pipeline.del("tasks:inprogress");
      pipeline.del("tasks:queue");
      if (allTaskIdsToDelete.length > 0) {
        pipeline.hdel("redis-tasks", ...allTaskIdsToDelete);
      }
      await pipeline.exec();

      console.log("[forwardTask] Active and pending tasks deleted from Redis successfully.");

      // 2. Send 'Cancel' task directly to the robot (via RabbitMQ)
      const { getActiveRabbitMqClient, createRabbitMqClient } = require("../clients/rabbitmq");
      const { env } = require("../config/env");

      let client = getActiveRabbitMqClient();
      if (!client) {
        const { createLogger } = require("../lib/logger");
        const logger = state.api?.logger || createLogger("controllers-tasks-rabbitmq");
        client = createRabbitMqClient({
          config: env.rabbitmq,
          logger: logger.child("rabbitmq"),
        });
        await client.connect().catch((error) => {
          logger.error("Failed to connect to RabbitMQ in forwardTask cancel fallback", error.message);
        });
      }

      if (client) {
        await client.publish({
          exchange: env.rabbitmq.tasksExchange,
          routingKey: env.rabbitmq.tasksRoutingKey,
          content: Buffer.from(""),
        });
        console.log("[forwardTask] Cancel task sent directly to robot via RabbitMQ.");
      } else {
        console.warn("[forwardTask] No RabbitMQ client available to send Cancel task.");
      }

      // Also emit socket event to notify frontend of the cleared/cancelled tasks
      try {
        const { getSocketInstance } = require("../realtime/socket");
        const socket = getSocketInstance();
        if (socket) {
          socket.emit("task-status-changed", {
            masterTaskName: "Cancel",
            status: "cancelled",
          });
        }
      } catch (err) {
        console.error("[forwardTask] Error emitting socket event for Cancel task:", err.message);
      }

      return res.status(200).json({
        message: "Cancel task processed: deleted all active/pending tasks and forwarded cancel signal.",
      });
    }
 
    const createdTask = await tasksModel.createTask(state.api.redis.client, task);
 
    console.log(`[forwardTask] Task created in Redis successfully. taskId: "${createdTask.taskId}"`);
 
    dispatch().catch((error) => {
      state.api.logger?.error("Error triggering dispatch after createTask:", error);
    });
 
    return res.status(201).json({
      message: "Task forwarded and stored in Redis successfully.",
      data: createdTask,
    });
  } catch (error) {
    console.error(`[forwardTask] Error occurred: ${error.message}`);
    return sendErrorResponse(res, error, state.api.logger);
  }
};

const getForwardTasks = async (req, res) => {
  try {
    if (!state.api.redis || !state.api.redis.client) {
      throw new Error("Redis client is not initialized.");
    }
 
    const tasks = await tasksModel.getAllForwardTasks(state.api.redis.client);
 
    return res.status(200).json({
      message: "Tasks fetched successfully.",
      data: tasks,
    });
  } catch (error) {
    return sendErrorResponse(res, error, state.api.logger);
  }
};

const editSchedule = async (req, res) => {
  try {
    const taskIds = Array.isArray(req.body) ? req.body : req.body?.taskIds;
 
    if (!Array.isArray(taskIds)) {
      throw new ValidationError("taskIds must be an array.");
    }
 
    if (!state.api.redis || !state.api.redis.client) {
      throw new Error("Redis client is not initialized.");
    }
 
    await tasksModel.updateSchedule(state.api.redis.client, taskIds);
 
    return res.status(200).json({
      message: "Schedule updated successfully.",
    });
  } catch (error) {
    return sendErrorResponse(res, error, state.api.logger);
  }
};

const deleteTask = async (req, res) => {
  const params = req.params || {};
  const { body } = req;
  const { query } = req;

  try {
    const result = await tasksModel.deleteByMasterTaskName(params.title);
    if (result.deletedCount === 0) {
      throw new NotFoundError(`Task '${params.title}' not found.`);
    }

    return res.status(200).json({
      message: `Task '${params.title}' deleted successfully`,
    });
  } catch (error) {
    return sendErrorResponse(res, error, state.api.logger);
  }
};

module.exports = {
  deleteTask,
  editSchedule,
  forwardTask,
  getForwardTasks,
  getAllTasks,
  getTasks,
  saveTask,
  sendTask,
};
