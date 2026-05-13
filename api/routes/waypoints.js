const express = require("express");
const { asyncHandler } = require("../../middlewares/async-handler");

function createWaypointsRouter({ handler }) {
  const router = express.Router();

  router.post("/save-waypoint", asyncHandler(handler.saveWaypoint));
  router.get("/get-all-waypoints", asyncHandler(handler.getAllWaypoints));

  return router;
}

module.exports = {
  createWaypointsRouter,
};
