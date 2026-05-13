const {
  ensureArray,
  ensureObject,
  ensureOptionalString,
  ensureString,
} = require("../../lib/validation");

function validateWaypointDocument(payload) {
  const body = payload.waypoint || payload;
  ensureObject(body, "waypoint");

  const neighbours = body.neighbour || [];
  if (neighbours.length > 0) {
    ensureArray(neighbours, "neighbour");
    for (const waypointName of neighbours) {
      ensureOptionalString(waypointName, "neighbour entry");
    }
  }

  return {
    waypointName: ensureString(body.waypointName, "waypointName"),
    cords: ensureArray(body.cords, "cords"),
    neighbour: Array.isArray(neighbours) ? neighbours : [],
  };
}

function createWaypointsHandler({ collection }) {
  return {
    async saveWaypoint(req, res) {
      const waypoint = validateWaypointDocument(req.body);
      const result = await collection.insertOne({
        ...waypoint,
        createdAt: new Date(),
      });
      const insertedId = result.insertedId;

      if (waypoint.neighbour.length > 0) {
        await collection.updateMany(
          { waypointName: { $in: waypoint.neighbour } },
          { $addToSet: { neighbour: waypoint.waypointName } }
        );
      }

      return res.status(201).json({
        message: "Waypoint saved successfully",
        insertedId,
      });
    },

    async getAllWaypoints(req, res) {
      const waypoints = await collection.find({}).toArray();
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
