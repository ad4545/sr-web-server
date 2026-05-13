const { TWIST_PUBLISH_OPTIONS } = require("../../config/constants");
const { ValidationError } = require("../../lib/errors");
const { ensureNumber, ensureObject } = require("../../lib/validation");

function safeAck(ack, payload) {
  if (typeof ack === "function") {
    ack(payload);
  }
}

function validateObjectKeys(value, allowedKeys, label) {
  const extraKeys = Object.keys(value).filter((key) => !allowedKeys.includes(key));
  if (extraKeys.length > 0) {
    throw new ValidationError(`${label} contains unsupported field(s): ${extraKeys.join(", ")}`);
  }
}

function validateVector3(value, label) {
  ensureObject(value, label);
  validateObjectKeys(value, ["X", "Y", "Z"], label);

  for (const axis of ["X", "Y", "Z"]) {
    ensureNumber(value[axis], `${label}.${axis}`);
  }
}

function validateTwistPayload(payload, twistType) {
  ensureObject(payload, "Twist payload");
  validateObjectKeys(payload, ["Linear", "Angular"], "Twist payload");

  if (!payload.Linear) {
    throw new ValidationError("Linear is required.");
  }

  if (!payload.Angular) {
    throw new ValidationError("Angular is required.");
  }

  validateVector3(payload.Linear, "Linear");
  validateVector3(payload.Angular, "Angular");

  const protobufError = twistType.verify(payload);
  if (protobufError) {
    throw new ValidationError(protobufError);
  }
}

function createTwistHandler({ rabbitMqClient, rabbitConfig, twistType, logger }) {
  return async function handleTwist(payload, ack) {
    try {
      validateTwistPayload(payload, twistType);

      const message = twistType.create(payload);
      const content = Buffer.from(twistType.encode(message).finish());

      await rabbitMqClient.publish({
        exchange: rabbitConfig.exchange,
        routingKey: rabbitConfig.routingKey,
        content,
        options: TWIST_PUBLISH_OPTIONS,
      });

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
