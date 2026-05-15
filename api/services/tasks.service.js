const { NotFoundError } = require("../../lib/errors");
const {
  validateTaskDocument,
  validateTaskPublishEnvelope,
} = require("../validators/tasks");

// OCP: adding a new task transport or repository should not require changing the use-case logic here.
function createTasksService({ repository, kafkaProducer, pageSize = 3 }) {
  function normalizePage(pageValue) {
    const page = Number.parseInt(pageValue, 10) || 1;
    return Number.isFinite(page) && page > 0 ? page : 1;
  }

  return {
    async saveTask(payload) {
      const task = validateTaskDocument(payload);
      return repository.saveTask({
        ...task,
        createdAt: new Date(),
      });
    },

    async getAllTasks() {
      return repository.listAll();
    },

    async getTasks(pageValue) {
      const currentPage = normalizePage(pageValue);
      const totalCount = await repository.count();
      const skip = (currentPage - 1) * pageSize;
      const tasks = await repository.listPage({ skip, limit: pageSize });
      const totalPages = Math.ceil(totalCount / pageSize);

      return {
        currentPage,
        totalPages,
        totalCount,
        hasNextPage: currentPage < totalPages,
        hasPrevPage: currentPage > 1,
        data: tasks,
      };
    },

    async sendTask(payload) {
      const task = validateTaskPublishEnvelope(payload);

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

      return task;
    },

    async deleteTask(title) {
      const result = await repository.deleteByMasterTaskName(title);

      if (result.deletedCount === 0) {
        throw new NotFoundError(`Task '${title}' not found.`);
      }

      return result;
    },
  };
}

module.exports = {
  createTasksService,
};
