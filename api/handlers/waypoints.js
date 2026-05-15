const { createWaypointsRepository } = require("../repositories/waypoints.repository");
const { createWaypointsService } = require("../services/waypoints.service");

function createWaypointsHandler({ collection, service }) {
  const waypointsService =
    service ||
    createWaypointsService({
      repository: createWaypointsRepository({ collection }),
    });

  return {
    async saveWaypoint(req, res) {
      const insertedId = await waypointsService.saveWaypoint(req.body);

      return res.status(201).json({
        message: "Waypoint saved successfully",
        insertedId,
      });
    },

    async getAllWaypoints(req, res) {
      const waypoints = await waypointsService.getAllWaypoints();
      return res.status(200).json({
        message: "Waypoints fetched successfully",
        count: waypoints.length,
        data: waypoints,
      });
    },
  };
}

module.exports = {
  createWaypointsHandler,
};
