const { ValidationError, sendErrorResponse } = require("../lib/errors");
const waypointsModel = require("../models/waypoints");
const { state } = require("../state/runtimeState");

const saveWaypoint = async (req, res) => {
  const { params } = req;
  const body = req.body;
  const { query } = req;

  try {
    const waypoint = body?.waypoint || body;
    if (!waypoint || typeof waypoint !== "object" || Array.isArray(waypoint)) {
      throw new ValidationError("waypoint must be an object.");
    }
    if (typeof waypoint.waypointName !== "string" || waypoint.waypointName.length === 0) {
      throw new ValidationError("waypointName must be a non-empty string.");
    }
    if (!Array.isArray(waypoint.cords)) {
      throw new ValidationError("cords must be an array.");
    }

    const neighbour = waypoint.neighbour || [];
    if (!Array.isArray(neighbour)) {
      throw new ValidationError("neighbour must be an array.");
    }
    for (const waypointName of neighbour) {
      if (waypointName !== undefined && waypointName !== null && typeof waypointName !== "string") {
        throw new ValidationError("neighbour entry must be a string.");
      }
    }

    const insertedId = await waypointsModel.saveWaypoint({
      waypointName: waypoint.waypointName,
      cords: waypoint.cords,
      neighbour,
      createdAt: new Date(),
    });

    if (neighbour.length > 0) {
      await waypointsModel.linkNeighbours(neighbour, waypoint.waypointName);
    }

    return res.status(201).json({
      message: "Waypoint saved successfully",
      insertedId,
    });
  } catch (error) {
    return sendErrorResponse(res, error, state.api.logger);
  }
};

const getAllWaypoints = async (req, res) => {
  const { params } = req;
  const { body } = req;
  const { query } = req;

  try {
    const waypoints = await waypointsModel.listAll();
    return res.status(200).json({
      message: "Waypoints fetched successfully",
      count: waypoints.length,
      data: waypoints,
    });
  } catch (error) {
    return sendErrorResponse(res, error, state.api.logger);
  }
};

module.exports = {
  getAllWaypoints,
  saveWaypoint,
};
