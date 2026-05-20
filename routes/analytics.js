const express = require("express");
const { getOverallMetrics } = require("../controllers/analytics");

const router = express.Router();

router.get("/overall-metrics", getOverallMetrics);

module.exports = router;
