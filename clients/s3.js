const AWS = require("aws-sdk");

const PLACEHOLDER_VALUES = new Set(["your-access-key-id", "your-secret-access-key"]);

const shouldUseStaticCredentials = (config) => {
  return Boolean(
    config.accessKeyId &&
      config.secretAccessKey &&
      !PLACEHOLDER_VALUES.has(config.accessKeyId) &&
      !PLACEHOLDER_VALUES.has(config.secretAccessKey)
  );
};

const createS3Client = ({ config }) => {
  const s3Config = {
    region: config.region,
  };

  if (shouldUseStaticCredentials(config)) {
    s3Config.accessKeyId = config.accessKeyId;
    s3Config.secretAccessKey = config.secretAccessKey;
  }

  return new AWS.S3(s3Config);
};

module.exports = {
  createS3Client,
  shouldUseStaticCredentials,
};
