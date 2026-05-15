const { createTasksRepository } = require("../repositories/tasks.repository");
const { createTasksService } = require("../services/tasks.service");

function createTasksHandler({ collection, kafkaProducer, pageSize = 3, service }) {
  const tasksService =
    service ||
    createTasksService({
      repository: createTasksRepository({ collection }),
      kafkaProducer,
      pageSize,
    });

  return {
    async saveTask(req, res) {
      const insertedId = await tasksService.saveTask(req.body);
      return res.status(201).json({
        message: "Task saved successfully",
        insertedId,
      });
    },

    async getAllTasks(req, res) {
      const tasks = await tasksService.getAllTasks();
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
      const pagination = await tasksService.getTasks(req.query.page);

      return res.status(200).json({
        message: "Tasks fetched successfully",
        ...pagination,
      });
    },

    async sendTask(req, res) {
      const task = await tasksService.sendTask(req.body.task);

      return res.status(200).json({
        message: `Task published to Kafka topic '${task.topic}' successfully`,
      });
    },

    async deleteTask(req, res) {
      await tasksService.deleteTask(req.params.title);

      return res.status(200).json({
        message: `Task '${req.params.title}' deleted successfully`,
      });
    },
  };
}

module.exports = {
  createTasksHandler,
};
