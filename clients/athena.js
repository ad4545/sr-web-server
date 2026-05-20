const { AthenaClient } = require("@aws-sdk/client-athena");

const PLACEHOLDER_VALUES = new Set(["your-access-key-id", "your-secret-access-key"]);

const shouldUseStaticCredentials = (config) => {
  return Boolean(
    config.accessKeyId &&
      config.secretAccessKey &&
      !PLACEHOLDER_VALUES.has(config.accessKeyId) &&
      !PLACEHOLDER_VALUES.has(config.secretAccessKey)
  );
};

const createAthenaClient = ({ awsConfig, athenaConfig }) => {
  const clientConfig = {
    region: athenaConfig.region || awsConfig.region,
  };

  if (shouldUseStaticCredentials(awsConfig)) {
    clientConfig.credentials = {
      accessKeyId: awsConfig.accessKeyId,
      secretAccessKey: awsConfig.secretAccessKey,
    };
  }

  return new AthenaClient(clientConfig);
};

module.exports = {
  createAthenaClient,
};
