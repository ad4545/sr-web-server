const state = {
  role: null,
  api: {
    ready: false,
    logger: null,
    mongo: null,
    redis: null,
    kafkaProducer: null,
    s3: null,
  },
  realtime: {
    ready: false,
    logger: null,
    rabbitMqClient: null,
    socket: null,
    streams: {},
    taskCompletionTracker: null,
  },
};

const setRole = (role) => {
  state.role = role;
};

const setApiState = (updates) => {
  Object.assign(state.api, updates);
};

const setRealtimeState = (updates) => {
  Object.assign(state.realtime, updates);
};

module.exports = {
  state,
  setApiState,
  setRealtimeState,
  setRole,
};
