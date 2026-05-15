// SRP: encapsulate MongoDB path persistence in a dedicated repository port.
function createPathsRepository({ collection }) {
  return {
    async savePath(path) {
      const result = await collection.insertOne(path);
      return result.insertedId;
    },

    async updatePath(pathName, paths) {
      return collection.updateOne(
        { pathName },
        {
          $set: {
            paths,
            updatedAt: new Date(),
          },
        }
      );
    },

    async listAll() {
      return collection.find({}).toArray();
    },
  };
}

module.exports = {
  createPathsRepository,
};
