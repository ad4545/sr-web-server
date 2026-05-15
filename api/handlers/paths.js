const { createPathsRepository } = require("../repositories/paths.repository");
const { createPathsService } = require("../services/paths.service");

function createPathsHandler({ collection, service }) {
  const pathsService =
    service ||
    createPathsService({
      repository: createPathsRepository({ collection }),
    });

  return {
    async savePath(req, res) {
      const insertedId = await pathsService.savePath(req.body);
      return res.status(201).json({
        message: "Path saved successfully",
        insertedId,
      });
    },

    async updatePath(req, res) {
      const { pathName } = req.params;
      const updated = await pathsService.updatePath(pathName, req.body.paths);

      if (!updated) {
        return res.status(404).json({
          message: `Path with name '${pathName}' not found`,
        });
      }

      return res.status(200).json({
        message: "Path updated successfully",
      });
    },

    async getPaths(req, res) {
      const paths = await pathsService.getPaths();
      return res.status(200).json({
        message: "Paths fetched successfully",
        count: paths.length,
        data: paths,
      });
    },
  };
}

module.exports = {
  createPathsHandler,
};
