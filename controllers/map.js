const { MAP_CACHE_KEY } = require("../config/constants");
const { sendErrorResponse } = require("../lib/errors");
const mapModel = require("../models/map");
const { state } = require("../state/runtimeState");

const getMap = async (req, res) => {
  const { params } = req;
  const { body } = req;
  const { query } = req;

  try {
    const logger = state.api.logger ? state.api.logger.child("map") : null;
    const cachedValue = await mapModel.getCachedMap(MAP_CACHE_KEY);
    if (cachedValue) {
      if (logger) {
        logger.info("Serving map from Redis cache");
      }

      return res.status(200).send(Buffer.from(cachedValue, "base64"));
    }

    if (logger) {
      logger.info("Fetching map from S3");
    }

    const mapBuffer = await mapModel.readMap();
    await mapModel.setCachedMap(mapBuffer.toString("base64"), MAP_CACHE_KEY);

    return res.status(200).send(mapBuffer);
  } catch (error) {
    return sendErrorResponse(res, error, state.api.logger);
  }
};

module.exports = {
  getMap,
};
