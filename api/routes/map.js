const express = require("express");
const { asyncHandler } = require("../../middlewares/async-handler");

function createMapRouter({ handler }) {
  const router = express.Router();

  router.get("/get-map", asyncHandler(handler.getMap));

  return router;
}

module.exports = {
  createMapRouter,
};
