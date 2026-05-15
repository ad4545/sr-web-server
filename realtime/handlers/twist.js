const { ValidationError } = require("../../lib/errors");
const { createTwistService } = require("../services/twist.service");

function safeAck(ack, payload) {
  if (typeof ack === "function") {
    ack(payload);
  }
}

function createTwistHandler({ rabbitMqClient, rabbitConfig, twistType, logger, service }) {
  const twistService =
    service ||
    createTwistService({
      rabbitMqClient,
      rabbitConfig,
      twistType,
    });

  return async function handleTwist(payload, ack) {
    try {
      await twistService.publishTwist(payload);

      safeAck(ack, { ok: true });
    } catch (error) {
      if (error instanceof ValidationError) {
        logger.warn("Invalid twist payload", error.message);
        safeAck(ack, { ok: false, error: error.message });
        return;
      }

      logger.error("Failed to publish twist", error.message);
      safeAck(ack, { ok: false, error: "Failed to publish twist message." });
    }
  };
}

module.exports = {
  createTwistHandler,
};
