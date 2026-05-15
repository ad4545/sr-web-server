const { ensureArray, ensureObject, ensureString } = require("../../lib/validation");

// SRP: keep path payload validation separate from persistence and HTTP concerns.
function validatePathDocument(payload) {
  const body = ensureObject(payload.path, "path");

  return {
    pathName: ensureString(body.pathName, "pathName"),
    paths: ensureArray(body.paths, "paths"),
  };
}

module.exports = {
  validatePathDocument,
};
