function buildRobotTopics(robots, suffixes) {
  return robots.flatMap((robot) => suffixes.map((suffix) => `${robot}.${suffix}`));
}

function resolveRobotIdFromTopic(topic, suffixes) {
  for (const suffix of suffixes) {
    const topicSuffix = `.${suffix}`;
    if (topic.endsWith(topicSuffix)) {
      return topic.slice(0, -topicSuffix.length);
    }
  }

  return topic;
}

module.exports = {
  buildRobotTopics,
  resolveRobotIdFromTopic,
};
