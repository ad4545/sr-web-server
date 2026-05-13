const { Server } = require("socket.io");

function createSocketServer({ httpServer, logger }) {
  const io = new Server(httpServer, {
    cors: {
      origin: "*",
      methods: ["GET", "POST"],
    },
  });

  let twistHandler = null;

  io.on("connection", (socket) => {
    logger.info(`Socket connected: ${socket.id}`);

    socket.on("twist", (...args) => {
      if (!twistHandler) {
        return;
      }

      Promise.resolve(twistHandler(...args)).catch((error) => {
        logger.error("twist handler failed", error.message);
      });
    });

    socket.on("disconnect", () => {
      logger.info(`Socket disconnected: ${socket.id}`);
    });
  });

  return {
    onTwist(handler) {
      twistHandler = handler;
    },
    emitPosition(payload) {
      io.emit("position", payload);
    },
    emitTaskFeedback(payload) {
      io.emit("task_feedback", payload);
    },
    emitBattery(payload) {
      io.emit("battery", payload);
    },
    async close() {
      await io.close();
    },
  };
}

module.exports = {
  createSocketServer,
};
