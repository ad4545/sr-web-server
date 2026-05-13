const AWS = require("aws-sdk");

function createS3Client({ config }) {
  return new AWS.S3({
    accessKeyId: config.accessKeyId,
    secretAccessKey: config.secretAccessKey,
    region: config.region,
  });
}

module.exports = {
  createS3Client,
};
