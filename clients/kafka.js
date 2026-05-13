const { Kafka } = require("kafkajs");

function createKafkaClient({ config }) {
  const kafka = new Kafka({
    clientId: config.clientId,
    brokers: config.brokers,
  });

  return {
    createProducer() {
      return kafka.producer();
    },
    createConsumer(groupId) {
      return kafka.consumer({ groupId });
    },
  };
}

module.exports = {
  createKafkaClient,
};
