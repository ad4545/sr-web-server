const express = require("express");
const {
  deleteTask,
  editSchedule,
  forwardTask,
  getForwardTasks,
  getAllTasks,
  getTasks,
  saveTask,
  sendTask,
} = require("../controllers/tasks");

const router = express.Router();
router.post("/save-task", saveTask);
router.get("/get-all-tasks", getAllTasks);
router.get("/get-tasks", getTasks);
router.post("/send-task", sendTask);
router.delete("/delete-task/:title", deleteTask);

router.post("/forward-task", forwardTask);
router.get("/get-forward-tasks", getForwardTasks);
router.post("/edit-schedule", editSchedule);

module.exports = router;
