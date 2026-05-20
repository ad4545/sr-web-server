const { env } = require("../config/env");
const { state } = require("../state/runtimeState");

const getCollection = () => {
  if (!state.api.mongo) {
    throw new Error("MongoDB connection is not initialized.");
  }

  return state.api.mongo.collection(env.mongo.collections.paths);
};

const savePath = async (path) => {
  const result = await getCollection().insertOne(path);
  return result.insertedId;
};

const updatePath = async (pathName, paths) => {
  return getCollection().updateOne(
    { pathName },
    {
      $set: {
        paths,
        updatedAt: new Date(),
      },
    }
  );
};

const listAll = async () => {
  return getCollection().find({}).toArray();
};

module.exports = {
  listAll,
  savePath,
  updatePath,
};
