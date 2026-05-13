const express = require("express");
const { asyncHandler } = require("../../middlewares/async-handler");

function createPathsRouter({ handler }) {
  const router = express.Router();

  router.post("/save-path", asyncHandler(handler.savePath));
  router.patch("/update-path/:pathName", asyncHandler(handler.updatePath));
  router.get("/get-paths", asyncHandler(handler.getPaths));

  return router;
}

module.exports = {
  createPathsRouter,
};
