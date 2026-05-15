const { MAP_CACHE_KEY } = require("../../config/constants");
const { createMapService } = require("../services/map.service");

function createMapHandler({ redisClient, s3, bucketName, key, logger, service }) {
  const mapService =
    service ||
    createMapService({
      cache: redisClient
        ? {
            // DIP: keep Redis access behind a tiny cache port.
            async get(cacheKey) {
              return redisClient.get(cacheKey);
            },

            async set(cacheKey, value) {
              return redisClient.set(cacheKey, value);
            },
          }
        : null,
      objectStore: {
        // DIP: keep S3 access behind a narrow object-store port.
        async read({ bucketName: serviceBucketName, key: serviceKey }) {
          const response = await s3.getObject({
            Bucket: serviceBucketName,
            Key: serviceKey,
          }).promise();
          return response.Body;
        },
      },
      bucketName,
      key,
      logger,
      cacheKey: MAP_CACHE_KEY,
    });

  return {
    async getMap(req, res) {
      const mapBuffer = await mapService.getMap();
      return res.status(200).send(mapBuffer);
    },
  };
}

module.exports = {
  createMapHandler,
};
