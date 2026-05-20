const { MongoClient } = require("mongodb");

const createMongoConnection = async ({ uri, dbName, logger }) => {
  const client = new MongoClient(uri);
  await client.connect();

  const db = client.db(dbName);
  logger.info("MongoDB connected");

  return {
    db,
    collection(name) {
      return db.collection(name);
    },
    async close() {
      await client.close();
      logger.info("MongoDB connection closed");
    },
  };
};

module.exports = {
  createMongoConnection,
};
