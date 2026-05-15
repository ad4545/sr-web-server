const {
  ensureArray,
  ensureObject,
  ensureOptionalString,
  ensureString,
} = require("../../lib/validation");

// SRP: validate waypoint payloads without coupling validation to Mongo or HTTP.
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

module.exports = {
  validateWaypointDocument,
};
