const express = require("express");
const { getPaths, savePath, updatePath } = require("../controllers/paths");

const router = express.Router();

router.post("/save-path", savePath);
router.patch("/update-path/:pathName", updatePath);
router.get("/get-paths", getPaths);

module.exports = router;
