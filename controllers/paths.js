const { ValidationError, sendErrorResponse } = require("../lib/errors");
const pathsModel = require("../models/paths");
const { state } = require("../state/runtimeState");

const savePath = async (req, res) => {
  const { params } = req;
  const body = req.body;
  const { query } = req;

  try {
    const path = body?.path;
    if (!path || typeof path !== "object" || Array.isArray(path)) {
      throw new ValidationError("path must be an object.");
    }
    if (typeof path.pathName !== "string" || path.pathName.length === 0) {
      throw new ValidationError("pathName must be a non-empty string.");
    }
    if (!Array.isArray(path.paths)) {
      throw new ValidationError("paths must be an array.");
    }

    const insertedId = await pathsModel.savePath({
      pathName: path.pathName,
      paths: path.paths,
      createdAt: new Date(),
    });
    return res.status(201).json({
      message: "Path saved successfully",
      insertedId,
    });
  } catch (error) {
    return sendErrorResponse(res, error, state.api.logger);
  }
};

const updatePath = async (req, res) => {
  const params = req.params || {};
  const body = req.body;
  const { query } = req;

  try {
    const result = await pathsModel.updatePath(params.pathName, body.paths);
    const updated = result.matchedCount > 0;

    if (!updated) {
      return res.status(404).json({
        message: `Path with name '${params.pathName}' not found`,
      });
    }

    return res.status(200).json({
      message: "Path updated successfully",
    });
  } catch (error) {
    return sendErrorResponse(res, error, state.api.logger);
  }
};

const getPaths = async (req, res) => {
  const { params } = req;
  const { body } = req;
  const { query } = req;

  try {
    const paths = await pathsModel.listAll();
    return res.status(200).json({
      message: "Paths fetched successfully",
      count: paths.length,
      data: paths,
    });
  } catch (error) {
    return sendErrorResponse(res, error, state.api.logger);
  }
};

module.exports = {
  getPaths,
  savePath,
  updatePath,
};
