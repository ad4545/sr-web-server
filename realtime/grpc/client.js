const http2 = require("http2");
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

function createGrpcTopicStream({
  grpcConfig,
  logger,
  label,
  topics,
  topicStreamRequestType,
  rawDataChunkType,
  onChunk,
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

  function handleChunk(messageBuffer, requestedTopic) {
    const rawChunk = rawDataChunkType.decode(messageBuffer);
    const topic = rawChunk.topic || requestedTopic;
    const payload = rawChunk.payload ? Buffer.from(rawChunk.payload) : Buffer.alloc(0);

    if (payload.length === 0) {
      logger.warn(`${label} received empty payload`, { topic });
      return;
    }

    onChunk({
      topic,
      key: rawChunk.key || null,
      payload,
      rawChunk,
    });
  }

  function subscribeToTopicOnce(topic) {
    return new Promise((resolve, reject) => {
      let settled = false;
      let grpcStatus = null;
      let grpcMessage = null;

      function settle(callback, value) {
        if (settled) {
          return;
        }

        settled = true;
        callback(value);
      }

      const session = http2.connect(authority);
      activeSessions.add(session);

      const fail = (error) => {
        cleanupSession(session);
        markDisconnected(topic);

        if (abortController.signal.aborted) {
          settle(resolve);
          return;
        }

        try {
          session.destroy();
        } catch (destroyError) {
          logger.debug(`${label} failed to destroy grpc session`, destroyError);
        }

        setLastError(error);
        settle(reject, error);
      };

      session.once("error", fail);
      session.once("close", () => {
        cleanupSession(session);
        markDisconnected(topic);

        if (abortController.signal.aborted) {
          settle(resolve);
          return;
        }

        if (!settled) {
          const error = new Error(`grpc session closed before completion for topic ${topic}`);
          setLastError(error);
          settle(reject, error);
        }
      });

      session.once("connect", () => {
        const requestPayload = topicStreamRequestType
          .encode(topicStreamRequestType.create({ topic }))
          .finish();

        const stream = session.request({
          ":method": "POST",
          ":path": "/com.example.grpc.StreamRouter/SubscribeToTopic",
          ":scheme": grpcConfig.useTls ? "https" : "http",
          ":authority": `${grpcConfig.host}:${grpcConfig.port}`,
          "content-type": "application/grpc+proto",
          te: "trailers",
        });

        const parser = createGrpcFrameParser((messageBuffer) => {
          try {
            handleChunk(messageBuffer, topic);
          } catch (error) {
            logger.error(`${label} failed to decode grpc payload`, {
              topic,
              error: error.message,
            });
          }
        });

        stream.once("response", (headers) => {
          const status = Number(headers[":status"]);
          if (status !== 200) {
            const error = new Error(`unexpected grpc http status ${status} for topic ${topic}`);
            error.silent = status === 404;
            fail(error);

            try {
              stream.close(http2.constants.NGHTTP2_CANCEL);
            } catch (closeError) {
              logger.debug(`${label} failed to close grpc stream`, closeError);
            }
            return;
          }

          markConnected(topic);
          state.lastError = null;
        });

        stream.on("data", (chunk) => {
          try {
            parser.push(chunk);
          } catch (error) {
            fail(error);
          }
        });

        stream.once("trailers", (trailers) => {
          grpcStatus = trailers["grpc-status"];
          grpcMessage = trailers["grpc-message"];
        });

        stream.once("error", fail);
        stream.once("end", () => {
          cleanupSession(session);
          markDisconnected(topic);

          if (abortController.signal.aborted) {
            settle(resolve);
            return;
          }

          if (grpcStatus && grpcStatus !== "0") {
            const error = new Error(
              `grpc stream ended with status ${grpcStatus} for topic ${topic}${
                grpcMessage ? `: ${grpcMessage}` : ""
              }`
            );
            error.grpcStatus = grpcStatus;
            error.grpcTopic = topic;
            error.silent = grpcStatus === "5";
            error.recoverable = true;
            setLastError(error);
            settle(reject, error);
            return;
          }

          settle(resolve);
        });

        stream.end(encodeGrpcMessage(requestPayload));
      });
    });
  }

  async function subscribeLoop(topic) {
    while (!abortController.signal.aborted) {
      try {
        await subscribeToTopicOnce(topic);
      } catch (error) {
        if (!abortController.signal.aborted && !error.silent) {
          logger.error(`${label} subscription failed`, {
            topic,
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

      if (!topics.length) {
        throw new Error(`${label} requires at least one topic`);
      }

      state.started = true;
      logger.info(`${label} starting`, {
        authority,
        topics,
      });

      for (const topic of topics) {
        const loopPromise = subscribeLoop(topic);
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

module.exports = {
  createGrpcTopicStream,
};
