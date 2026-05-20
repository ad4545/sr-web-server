const { resolveRobotIdFromTopic } = require("../config/grpcUtils");

const parseTaskFeedbackPayload = (payload) => {
  if (!payload || typeof payload.Data !== "string") {
    throw new Error("task feedback payload is missing Data");
  }

  const parsed = JSON.parse(payload.Data);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("task feedback payload must be an object");
  }

  const taskEntries = Object.entries(parsed);
  if (!taskEntries.length) {
    throw new Error("task feedback payload does not contain any task data");
  }

  const [taskID, taskFeedback] = taskEntries[0];

  if (typeof taskID !== "string" || !taskID) {
    throw new Error("task feedback payload is missing task ID");
  }

  if (!taskFeedback || typeof taskFeedback !== "object" || Array.isArray(taskFeedback)) {
    throw new Error("task feedback payload must contain an object for the task");
  }

  if (typeof taskFeedback.status !== "string" || !taskFeedback.status) {
    throw new Error("task feedback payload is missing status");
  }

  return {
    taskID,
    status: taskFeedback.status,
    feedback: taskFeedback,
  };
};

const createRealtimeStreamDefinitions = ({ grpcConfig, logger, socket, taskCompletionTracker }) => {
  return [
    {
      name: "odom",
      label: "robot-position stream",
      topicKey: "odom",
      schemaKey: "odom",
      onMessage({ topic, decoded }) {
        try {
          const robotId = resolveRobotIdFromTopic(topic, grpcConfig.topicSuffixes?.odom || []);
          socket.emitPosition({
            robotId,
            ...decoded,
          });
        } catch (error) {
          logger.error("robot-position stream processing failed", error.message);
        }
      },
    },
    {
      name: "taskFeedback",
      label: "task-feedback stream",
      topicKey: "taskFeedback",
      schemaKey: "taskFeedback",
      onMessage({ topic, decoded }) {
        try {
          const robotID = resolveRobotIdFromTopic(topic, grpcConfig.topicSuffixes?.taskFeedback || []);
          socket.emitTaskFeedback(decoded);
          taskCompletionTracker.handleTaskFeedback({
            ...parseTaskFeedbackPayload(decoded),
            robotID,
          });
        } catch (error) {
          logger.warn("task-feedback stream processing failed", error.message);
        }
      },
    },
    {
      name: "battery",
      label: "battery stream",
      topicKey: "battery",
      schemaKey: "battery",
      onMessage({ topic, decoded }) {
        try {
          const robotID = resolveRobotIdFromTopic(topic, grpcConfig.topicSuffixes?.battery || []);
          taskCompletionTracker.updateBattery({
            robotID,
            soc: decoded.SOC,
          });
          socket.emitBattery(decoded);
        } catch (error) {
          logger.error("battery stream processing failed", error.message);
        }
      },
    },
    {
      name: "speed",
      label: "speed stream",
      topicKey: "speed",
      schemaKey: "speed",
      onMessage: (() => {
        // Track first message per robot to avoid log flooding (cmd_vel fires at 10 Hz)
        const firstSeenRobots = new Set();
        return function onSpeedMessage({ topic, decoded }) {
          try {
            const robotID = resolveRobotIdFromTopic(topic, grpcConfig.topicSuffixes?.speed || []);

            if (!decoded.Linear || typeof decoded.Linear.X !== "number") {
              logger.warn("speed stream: decoded.Linear.X is missing or not a number", {
                topic,
                robotID,
                linear: decoded.Linear,
              });
              return;
            }

            if (!firstSeenRobots.has(robotID)) {
              firstSeenRobots.add(robotID);
            }

            taskCompletionTracker.updateSpeed({
              robotID,
              linearX: decoded.Linear.X,
            });
          } catch (error) {
            logger.error("speed stream processing failed", error.message);
          }
        };
      })(),
    },
  ];
};

module.exports = {
  createRealtimeStreamDefinitions,
  parseTaskFeedbackPayload,
};
