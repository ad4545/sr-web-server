// SRP: keep waypoint collection access behind a narrow repository boundary.
function createWaypointsRepository({ collection }) {
  return {
    async saveWaypoint(waypoint) {
      const result = await collection.insertOne(waypoint);
      return result.insertedId;
    },

    async linkNeighbours(neighbourNames, waypointName) {
      return collection.updateMany(
        { waypointName: { $in: neighbourNames } },
        { $addToSet: { neighbour: waypointName } }
      );
    },

    async listAll() {
      return collection.find({}).toArray();
    },
  };
}

module.exports = {
  createWaypointsRepository,
};
