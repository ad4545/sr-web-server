const { ValidationError } = require("./errors");

function assertCondition(condition, message, details) {
  if (!condition) {
    throw new ValidationError(message, details);
  }
}

function ensureObject(value, label) {
  assertCondition(
    value && typeof value === "object" && !Array.isArray(value),
    `${label} must be an object.`
  );
  return value;
}

function ensureString(value, label) {
  assertCondition(
    typeof value === "string" && value.trim().length > 0,
    `${label} is required and must be a string.`
  );
  return value.trim();
}

function ensureArray(value, label) {
  assertCondition(
    Array.isArray(value) && value.length > 0,
    `${label} is required and must be a non-empty array.`
  );
  return value;
}

function ensureNumber(value, label) {
  assertCondition(typeof value === "number" && !Number.isNaN(value), `${label} must be a valid number.`);
  return value;
}

function ensureOptionalString(value, label) {
  if (value === undefined || value === null || value === "") {
    return null;
  }

  return ensureString(value, label);
}

module.exports = {
  assertCondition,
  ensureArray,
  ensureNumber,
  ensureObject,
  ensureOptionalString,
  ensureString,
};
