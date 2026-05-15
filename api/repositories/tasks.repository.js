// SRP: isolate MongoDB persistence behind a task-specific repository port.
function createTasksRepository({ collection }) {
  return {
    async saveTask(task) {
      const result = await collection.insertOne(task);
      return result.insertedId;
    },

    async listAll() {
      return collection.find({}).toArray();
    },

    async count() {
      return collection.countDocuments();
    },

    async listPage({ skip, limit }) {
      return collection.find({}).skip(skip).limit(limit).toArray();
    },

    async deleteByMasterTaskName(masterTaskName) {
      return collection.deleteOne({
        masterTaskName,
      });
    },
  };
}

module.exports = {
  createTasksRepository,
};
