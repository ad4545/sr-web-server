const { MAP_CACHE_KEY } = require("../../config/constants");

function createMapHandler({ redisClient, s3, bucketName, key, logger }) {
  return {
    async getMap(req, res) {
      const cachedValue = redisClient ? await redisClient.get(MAP_CACHE_KEY) : null;
      if (cachedValue) {
        logger.info("Serving map from Redis cache");
        return res.status(200).send(Buffer.from(cachedValue, "base64"));
      }

      logger.info("Fetching map from S3");
      const response = await s3.getObject({
        Bucket: bucketName,
        Key: key,
      }).promise();

      if (redisClient) {
        await redisClient.set(MAP_CACHE_KEY, response.Body.toString("base64"));
      }

      return res.status(200).send(response.Body);
    },
  };
}

module.exports = {
  createMapHandler,
};
