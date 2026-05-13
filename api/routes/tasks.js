const express = require("express");
const { asyncHandler } = require("../../middlewares/async-handler");

function createTasksRouter({ handler }) {
  const router = express.Router();

  router.post("/save-task", asyncHandler(handler.saveTask));
  router.get("/get-all-tasks", asyncHandler(handler.getAllTasks));
  router.get("/get-tasks", asyncHandler(handler.getTasks));
  router.post("/send-task", asyncHandler(handler.sendTask));
  router.delete("/delete-task/:title", asyncHandler(handler.deleteTask));

  return router;
}

module.exports = {
  createTasksRouter,
};
