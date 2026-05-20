const express = require("express");
const { getMap } = require("../controllers/map");

const router = express.Router();

router.get("/get-map", getMap);

module.exports = router;
