const { ensureArray, ensureObject, ensureString } = require("../../lib/validation");

function validatePathDocument(payload) {
  const body = ensureObject(payload.path, "path");

  return {
    pathName: ensureString(body.pathName, "pathName"),
    paths: ensureArray(body.paths, "paths"),
  };
}

function createPathsHandler({ collection }) {
  return {
    async savePath(req, res) {
      const path = validatePathDocument(req.body);
      const result = await collection.insertOne({
        ...path,
        createdAt: new Date(),
      });
      return res.status(201).json({
        message: "Path saved successfully",
        insertedId: result.insertedId,
      });
    },

    async updatePath(req, res) {
      const { pathName } = req.params;

      const paths = req.body.paths
      console.log('path received :', paths)

      const result = await collection.updateOne(
        { pathName },
        {
          $set: {
            paths,
            updatedAt: new Date(),
          },
        }
      );

      if (result.matchedCount === 0) {
        return res.status(404).json({
          message: `Path with name '${pathName}' not found`,
        });
      }

      return res.status(200).json({
        message: "Path updated successfully",
      });
    },

    async getPaths(req, res) {
      const paths = await collection.find({}).toArray();
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
