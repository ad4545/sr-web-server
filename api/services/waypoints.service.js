const { validateWaypointDocument } = require("../validators/waypoints");

// OCP: waypoint neighbor rules stay here while the storage implementation remains swappable.
function createWaypointsService({ repository }) {
  return {
    async saveWaypoint(payload) {
      const waypoint = validateWaypointDocument(payload);
      const insertedId = await repository.saveWaypoint({
        ...waypoint,
        createdAt: new Date(),
      });

      if (waypoint.neighbour.length > 0) {
        await repository.linkNeighbours(waypoint.neighbour, waypoint.waypointName);
      }

      return insertedId;
    },

    async getAllWaypoints() {
      return repository.listAll();
    },
  };
}

module.exports = {
  createWaypointsService,
};
