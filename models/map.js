const { MAP_CACHE_KEY } = require("../config/constants");
const { env } = require("../config/env");
const { state } = require("../state/runtimeState");

const getCachedMap = async (cacheKey = MAP_CACHE_KEY) => {
  if (!state.api.redis || !state.api.redis.client) {
    return null;
  }

  return state.api.redis.client.get(cacheKey);
};

const setCachedMap = async (value, cacheKey = MAP_CACHE_KEY) => {
  if (!state.api.redis || !state.api.redis.client) {
    return null;
  }

  return state.api.redis.client.set(cacheKey, value);
};

const readMap = async () => {
  if (!state.api.s3) {
    throw new Error("S3 client is not initialized.");
  }

  const response = await state.api.s3.getObject({
    Bucket: env.aws.bucketName,
    Key: env.aws.mapName,
  }).promise();

  return response.Body;
};

module.exports = {
  getCachedMap,
  readMap,
  setCachedMap,
};
