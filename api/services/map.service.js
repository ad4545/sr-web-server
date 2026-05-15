const { MAP_CACHE_KEY } = require("../../config/constants");

// DIP: the map use-case depends on cache/object-store ports, not on Redis or S3 directly.
function createMapService({ cache, objectStore, bucketName, key, logger, cacheKey = MAP_CACHE_KEY }) {
  return {
    async getMap() {
      const cachedValue = cache ? await cache.get(cacheKey) : null;
      if (cachedValue) {
        logger.info("Serving map from Redis cache");
        return Buffer.from(cachedValue, "base64");
      }

      logger.info("Fetching map from S3");
      const body = await objectStore.read({ bucketName, key });

      if (cache) {
        await cache.set(cacheKey, body.toString("base64"));
      }

      return body;
    },
  };
}

module.exports = {
  createMapService,
};
