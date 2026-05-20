const { TWIST_PUBLISH_OPTIONS } = require("../config/constants");
const { ValidationError } = require("../lib/errors");

const createTwistHandler = ({ rabbitMqClient, rabbitConfig, twistType, logger }) => {
  return async (payload, ack) => {
    try {
      if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
        throw new ValidationError("Twist payload must be an object.");
      }

      const payloadKeys = Object.keys(payload).filter((key) => !["Linear", "Angular"].includes(key));
      if (payloadKeys.length > 0) {
        throw new ValidationError(
          `Twist payload contains unsupported field(s): ${payloadKeys.join(", ")}`
        );
      }
      if (!payload.Linear) {
        throw new ValidationError("Linear is required.");
      }
      if (!payload.Angular) {
        throw new ValidationError("Angular is required.");
      }

      for (const [label, value] of [
        ["Linear", payload.Linear],
        ["Angular", payload.Angular],
      ]) {
        if (!value || typeof value !== "object" || Array.isArray(value)) {
          throw new ValidationError(`${label} must be an object.`);
        }

        const extraKeys = Object.keys(value).filter((key) => !["X", "Y", "Z"].includes(key));
        if (extraKeys.length > 0) {
          throw new ValidationError(`${label} contains unsupported field(s): ${extraKeys.join(", ")}`);
        }

        for (const axis of ["X", "Y", "Z"]) {
          if (typeof value[axis] !== "number" || Number.isNaN(value[axis])) {
            throw new ValidationError(`${label}.${axis} must be a number.`);
          }
        }
      }

      const protobufError = twistType.verify(payload);
      if (protobufError) {
        throw new ValidationError(protobufError);
      }

      const message = twistType.create(payload);
      const content = Buffer.from(twistType.encode(message).finish());

      await rabbitMqClient.publish({
        exchange: rabbitConfig.exchange,
        routingKey: rabbitConfig.routingKey,
        content,
        options: TWIST_PUBLISH_OPTIONS,
      });

      if (typeof ack === "function") {
        ack({ ok: true });
      }
    } catch (error) {
      if (error instanceof ValidationError) {
        logger.warn("Invalid twist payload", error.message);
        if (typeof ack === "function") {
          ack({ ok: false, error: error.message });
        }
        return;
      }

      logger.error("Failed to publish twist", error.message);
      if (typeof ack === "function") {
        ack({ ok: false, error: "Failed to publish twist message." });
      }
    }
  };
};

module.exports = {
  createTwistHandler,
};
