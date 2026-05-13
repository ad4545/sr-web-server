const { NotFoundError } = require("../../lib/errors");
const {
  assertCondition,
  ensureArray,
  ensureObject,
  ensureOptionalString,
  ensureString,
} = require("../../lib/validation");

function validateTaskPath(path) {
  if (path === undefined || path === null) {
    return null;
  }

  ensureObject(path, "path");
  ensureString(path.pathName, "path.pathName");
  const points = ensureArray(path.paths, "path.paths");

  for (const point of points) {
    ensureObject(point, "path point");
    assertCondition(point.translation && point.rotation, "Each path point must have translation and rotation.");
  }

  return path;
}

function validateTaskItems(tasks) {
  const normalizedTasks = ensureArray(tasks, "tasks");

  for (const task of normalizedTasks) {
    ensureObject(task, "task");
    ensureString(task.taskName, "Each task.taskName");
    ensureString(task.type, "Each task.type");
    validateTaskPath(task.path);
  }

  return normalizedTasks;
}

function validateTaskDocument(payload) {
  const body = ensureObject(payload, "task payload");
  return {
    masterTaskName: ensureString(body.masterTaskName, "masterTaskName"),
    tasks: validateTaskItems(body.tasks),
    topic: ensureOptionalString(body.topic, "topic"),
  };
}

function validateTaskPublishEnvelope(payload) {
  return payload;
}

function createTasksHandler({ collection, kafkaProducer, pageSize = 3 }) {
  return {
    async saveTask(req, res) {
      const task = validateTaskDocument(req.body);
      const result = await collection.insertOne({
        ...task,
        createdAt: new Date(),
      });
      return res.status(201).json({
        message: "Task saved successfully",
        insertedId: result.insertedId,
      });
    },

    async getAllTasks(req, res) {
      const tasks = await collection.find({}).toArray();
      if (tasks.length === 0) {
        return res.status(404).json({ message: "No tasks found." });
      }

      return res.status(200).json({
        message: "Tasks fetched successfully",
        count: tasks.length,
        data: tasks,
      });
    },

    async getTasks(req, res) {
      const page = Number.parseInt(req.query.page, 10) || 1;
      const currentPage = Number.isFinite(page) && page > 0 ? page : 1;
      const totalCount = await collection.countDocuments();
      const skip = (currentPage - 1) * pageSize;
      const tasks = await collection.find({}).skip(skip).limit(pageSize).toArray();
      const totalPages = Math.ceil(totalCount / pageSize);

      return res.status(200).json({
        message: "Tasks fetched successfully",
        currentPage,
        totalPages,
        totalCount,
        hasNextPage: currentPage < totalPages,
        hasPrevPage: currentPage > 1,
        data: tasks,
      });
    },

    async sendTask(req, res) {
      const task = validateTaskPublishEnvelope(req.body.task);

      await kafkaProducer.send({
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
    },

    async deleteTask(req, res) {
      const result = await collection.deleteOne({
        masterTaskName: req.params.title,
      });

      if (result.deletedCount === 0) {
        throw new NotFoundError(`Task '${req.params.title}' not found.`);
      }

      return res.status(200).json({
        message: `Task '${req.params.title}' deleted successfully`,
      });
    },
  };
}

module.exports = {
  createTasksHandler,
};
