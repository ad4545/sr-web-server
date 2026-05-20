const express = require("express");
const { getAllWaypoints, saveWaypoint } = require("../controllers/waypoints");

const router = express.Router();

router.post("/save-waypoint", saveWaypoint);
router.get("/get-all-waypoints", getAllWaypoints);

module.exports = router;
