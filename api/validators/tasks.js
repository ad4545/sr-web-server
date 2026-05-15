const { assertCondition, ensureArray, ensureObject, ensureOptionalString, ensureString } = require("../../lib/validation");

// SRP: this module only validates task payloads so the service layer can stay focused on orchestration.
function validateTaskPath(path) {
  if (path === undefined || path === null) {
    return null;
  }

  ensureObject(path, "path");
  ensureString(path.pathName, "path.pathName");
  const points = ensureArray(path.paths, "path.paths");

  for (const point of points) {
    ensureObject(point, "path point");
    assertCondition(point.translation && point.rotation, "Each path point must have translation and rotation.");
  }

  return path;
}

function validateTaskItems(tasks) {
  const normalizedTasks = ensureArray(tasks, "tasks");

  for (const task of normalizedTasks) {
    ensureObject(task, "task");
    ensureString(task.taskName, "Each task.taskName");
    ensureString(task.type, "Each task.type");
    validateTaskPath(task.path);
  }

  return normalizedTasks;
}

function validateTaskDocument(payload) {
  const body = ensureObject(payload, "task payload");
  return {
    masterTaskName: ensureString(body.masterTaskName, "masterTaskName"),
    tasks: validateTaskItems(body.tasks),
    topic: ensureOptionalString(body.topic, "topic"),
  };
}

function validateTaskPublishEnvelope(payload) {
  return payload;
}

module.exports = {
  validateTaskDocument,
  validateTaskItems,
  validateTaskPath,
  validateTaskPublishEnvelope,
};
