const http2 = require("http2");
const { PROTOBUF_TO_OBJECT_OPTIONS } = require("../../config/constants");
const { createGrpcFrameParser, encodeGrpcMessage } = require("./framing");

function delay(ms, signal) {
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }

    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, ms);

    function onAbort() {
      clearTimeout(timer);
      resolve();
    }

    signal.addEventListener("abort", onAbort, { once: true });
  });
}

function createGrpcAuthority(config) {
  return `${config.useTls ? "https" : "http"}://${config.host}:${config.port}`;
}

function createRequestHeaders({ grpcConfig, requestPath }) {
  return {
    ":method": "POST",
    ":path": requestPath,
    ":scheme": grpcConfig.useTls ? "https" : "http",
    ":authority": `${grpcConfig.host}:${grpcConfig.port}`,
    "content-type": "application/grpc+proto",
    te: "trailers",
  };
}

function decodeProtobufPayload(schema, payload) {
  const decodedMessage = schema.type.decode(payload);
  return schema.type.toObject(decodedMessage, schema.toObjectOptions || PROTOBUF_TO_OBJECT_OPTIONS);
}

function createGrpcSubscriptionClient({
  grpcConfig,
  logger,
  label,
  subscriptions,
  topicStreamRequestType,
  rawDataChunkType,
  onMessage,
  requestPath = "/com.example.grpc.StreamRouter/SubscribeToTopic",
}) {
  const authority = createGrpcAuthority(grpcConfig);
  const activeSessions = new Set();
  const abortController = new AbortController();
  const loopPromises = [];
  const state = {
    started: false,
    connectedTopics: new Set(),
    lastError: null,
  };

  function cleanupSession(session) {
    activeSessions.delete(session);
  }

  function markConnected(topic) {
    state.connectedTopics.add(topic);
  }

  function markDisconnected(topic) {
    state.connectedTopics.delete(topic);
  }

  function setLastError(error) {
    if (!error || error.silent) {
      return;
    }

    state.lastError = error.message;
  }

  function handleChunk(messageBuffer, requestedTopic, subscription) {
    const rawChunk = rawDataChunkType.decode(messageBuffer);
    const topic = rawChunk.topic || requestedTopic;
    const payload = rawChunk.payload ? Buffer.from(rawChunk.payload) : Buffer.alloc(0);

    if (payload.length === 0) {
      logger.warn(`${label} received empty payload`, { topic });
      return;
    }

    const decoded = decodeProtobufPayload(subscription.schema, payload);

    onMessage({
      topic,
      key: rawChunk.key || null,
      decoded,
      rawChunk,
      schema: subscription.schema,
    });
  }

  function openStream(session, subscription, requestedTopic, settle, reject) {
    const requestPayload = topicStreamRequestType.encode(
      topicStreamRequestType.create({ topic: requestedTopic })
    ).finish();

    const stream = session.request(createRequestHeaders({ grpcConfig, requestPath }));
    const parser = createGrpcFrameParser((messageBuffer) => {
      try {
        handleChunk(messageBuffer, requestedTopic, subscription);
      } catch (error) {
        logger.error(`${label} failed to decode grpc payload`, {
          topic: requestedTopic,
          error: error.message,
        });
      }
    });

    let grpcStatus = null;
    let grpcMessage = null;

    stream.once("response", (headers) => {
      const status = Number(headers[":status"]);
      if (status !== 200) {
        const error = new Error(`unexpected grpc http status ${status} for topic ${requestedTopic}`);
        error.silent = status === 404;
        reject(error);

        try {
          stream.close(http2.constants.NGHTTP2_CANCEL);
        } catch (closeError) {
          logger.debug(`${label} failed to close grpc stream`, closeError);
        }
        return;
      }

      markConnected(requestedTopic);
      state.lastError = null;
    });

    stream.on("data", (chunk) => {
      try {
        parser.push(chunk);
      } catch (error) {
        reject(error);
      }
    });

    stream.once("trailers", (trailers) => {
      grpcStatus = trailers["grpc-status"];
      grpcMessage = trailers["grpc-message"];
    });

    stream.once("error", reject);
    stream.once("end", () => {
      cleanupSession(session);
      markDisconnected(requestedTopic);

      if (abortController.signal.aborted) {
        settle();
        return;
      }

      if (grpcStatus && grpcStatus !== "0") {
        const error = new Error(
          `grpc stream ended with status ${grpcStatus} for topic ${requestedTopic}${
            grpcMessage ? `: ${grpcMessage}` : ""
          }`
        );
        error.grpcStatus = grpcStatus;
        error.grpcTopic = requestedTopic;
        error.silent = grpcStatus === "5";
        error.recoverable = true;
        setLastError(error);
        reject(error);
        return;
      }

      settle();
    });

    stream.end(encodeGrpcMessage(requestPayload));
  }

  function subscribeToTopicOnce(subscription) {
    const { topic } = subscription;

    return new Promise((resolve, reject) => {
      let settled = false;

      function settle() {
        if (settled) {
          return;
        }

        settled = true;
        resolve();
      }

      function fail(error) {
        if (settled) {
          return;
        }

        cleanupSession(session);
        markDisconnected(topic);

        if (abortController.signal.aborted) {
          settle();
          return;
        }

        try {
          session.destroy();
        } catch (destroyError) {
          logger.debug(`${label} failed to destroy grpc session`, destroyError);
        }

        setLastError(error);
        settled = true;
        reject(error);
      }

      const session = http2.connect(authority);
      activeSessions.add(session);

      session.once("error", fail);
      session.once("close", () => {
        cleanupSession(session);
        markDisconnected(topic);

        if (abortController.signal.aborted) {
          settle();
          return;
        }

        if (!settled) {
          const error = new Error(`grpc session closed before completion for topic ${topic}`);
          setLastError(error);
          settled = true;
          reject(error);
        }
      });

      session.once("connect", () => {
        openStream(session, subscription, topic, settle, fail);
      });
    });
  }

  async function subscribeLoop(subscription) {
    while (!abortController.signal.aborted) {
      try {
        await subscribeToTopicOnce(subscription);
      } catch (error) {
        if (!abortController.signal.aborted && !error.silent) {
          logger.error(`${label} subscription failed`, {
            topic: subscription.topic,
            error: error.message,
          });
        }
      }

      if (!abortController.signal.aborted) {
        await delay(grpcConfig.reconnectDelayMs, abortController.signal);
      }
    }
  }

  return {
    async start() {
      if (state.started) {
        return;
      }

      if (!subscriptions.length) {
        throw new Error(`${label} requires at least one topic`);
      }

      state.started = true;
      logger.info(`${label} starting`, {
        authority,
        topics: subscriptions.map((subscription) => subscription.topic),
      });

      for (const subscription of subscriptions) {
        const loopPromise = subscribeLoop(subscription);
        loopPromises.push(loopPromise);
      }
    },

    async stop() {
      abortController.abort();

      for (const session of activeSessions) {
        try {
          session.destroy();
        } catch (error) {
          logger.debug(`${label} failed to destroy grpc session during shutdown`, error);
        }
      }

      await Promise.allSettled(loopPromises);
    },

    getStatus() {
      return {
        started: state.started,
        connectedTopics: [...state.connectedTopics],
        lastError: state.lastError,
      };
    },
  };
}

function createGrpcTopicStream(options) {
  return createGrpcSubscriptionClient(options);
}

module.exports = {
  createGrpcSubscriptionClient,
  createGrpcTopicStream,
};
