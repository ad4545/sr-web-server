const { env } = require("../config/env");
const { state } = require("../state/runtimeState");

const getCollection = () => {
  if (!state.api.mongo) {
    throw new Error("MongoDB connection is not initialized.");
  }

  return state.api.mongo.collection(env.mongo.collections.waypoints);
};

const saveWaypoint = async (waypoint) => {
  const result = await getCollection().insertOne(waypoint);
  return result.insertedId;
};

const linkNeighbours = async (neighbourNames, waypointName) => {
  return getCollection().updateMany(
    { waypointName: { $in: neighbourNames } },
    { $addToSet: { neighbour: waypointName } }
  );
};

const listAll = async () => {
  return getCollection().find({}).toArray();
};

module.exports = {
  linkNeighbours,
  listAll,
  saveWaypoint,
};
