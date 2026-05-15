const { ValidationError } = require("../../lib/errors");
const { ensureNumber, ensureObject } = require("../../lib/validation");

// SRP: twist payload validation lives here so transport code only publishes validated messages.
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

module.exports = {
  validateObjectKeys,
  validateTwistPayload,
  validateVector3,
};
