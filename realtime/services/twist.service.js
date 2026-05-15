const { TWIST_PUBLISH_OPTIONS } = require("../../config/constants");
const { validateTwistPayload } = require("../validators/twist");

// OCP: publishing logic is isolated so new message brokers can be swapped in behind the same port.
function createTwistService({ rabbitMqClient, rabbitConfig, twistType }) {
  return {
    async publishTwist(payload) {
      validateTwistPayload(payload, twistType);

      const message = twistType.create(payload);
      const content = Buffer.from(twistType.encode(message).finish());

      await rabbitMqClient.publish({
        exchange: rabbitConfig.exchange,
        routingKey: rabbitConfig.routingKey,
        content,
        options: TWIST_PUBLISH_OPTIONS,
      });

      return message;
    },
  };
}

module.exports = {
  createTwistService,
};
