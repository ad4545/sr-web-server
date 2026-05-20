const buildRobotTopics = (robots, suffixes) => {
  return robots.flatMap((robot) => suffixes.map((suffix) => `${robot}.${suffix}`));
};

const resolveRobotIdFromTopic = (topic, suffixes) => {
  for (const suffix of suffixes) {
    const topicSuffix = `.${suffix}`;
    if (topic.endsWith(topicSuffix)) {
      return topic.slice(0, -topicSuffix.length);
    }
  }

  throw new Error(
    `Cannot resolve robot ID from topic "${topic}": no configured suffix matched [${suffixes.join(", ")}]`
  );
};

module.exports = {
  buildRobotTopics,
  resolveRobotIdFromTopic,
};
