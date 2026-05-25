const express = require("express");
const { getOverallMetrics, getRobotMetrics } = require("../controllers/analytics");

const router = express.Router();

router.get("/overall-metrics", getOverallMetrics);
router.get("/robot/:robotId", getRobotMetrics);

module.exports = router;
