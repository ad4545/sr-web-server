const express = require("express");
const { getHealth, getReady, getRoot } = require("../controllers/health");

const router = express.Router();

if (process.env.APP_ROLE === "api-core") {
  router.get("/", getRoot);
}

router.get("/internal/health", getHealth);
router.get("/internal/ready", getReady);

module.exports = router;
