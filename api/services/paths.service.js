const { validatePathDocument } = require("../validators/paths");

// ISP: expose only path-related use cases to the HTTP adapter.
function createPathsService({ repository }) {
  return {
    async savePath(payload) {
      const path = validatePathDocument(payload);
      return repository.savePath({
        ...path,
        createdAt: new Date(),
      });
    },

    async updatePath(pathName, paths) {
      const result = await repository.updatePath(pathName, paths);
      return result.matchedCount > 0;
    },

    async getPaths() {
      return repository.listAll();
    },
  };
}

module.exports = {
  createPathsService,
};
