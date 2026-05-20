const http2 = require("http2");
const { PROTOBUF_TO_OBJECT_OPTIONS } = require("../../config/constants");
const { createGrpcFrameParser, encodeGrpcMessage } = require("./framing");
const {
  createSilentHttpStatusError,
  createGrpcStatusError,
  isTransientConnectionError,
  shouldSuppressStartupWaitError,
} = require("./errors");

const buildAuthority = (config) => {
  return `${config.useTls ? "https" : "http"}://${config.host}:${config.port}`;
};

const buildRequestHeaders = (config, requestPath) => {
  return {
    ":method": "POST",
    ":path": requestPath,
    ":scheme": config.useTls ? "https" : "http",
    ":authority": `${config.host}:${config.port}`,
    "content-type": "application/grpc+proto",
    te: "trailers",
  };
};

const decodePayload = (schema, payload) => {
  const decodedMessage = schema.type.decode(payload);
  return schema.type.toObject(decodedMessage, schema.toObjectOptions || PROTOBUF_TO_OBJECT_OPTIONS);
};

const createRequestPayload = (topicStreamRequestType, topic) => {
  return topicStreamRequestType.encode(topicStreamRequestType.create({ topic })).finish();
};

const delay = (ms, signal) => {
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }

    const timer = setTimeout(() => {
      signal.removeEventListener("abort", handleAbort);
      resolve();
    }, ms);

    const handleAbort = () => {
      clearTimeout(timer);
      resolve();
    };

    signal.addEventListener("abort", handleAbort, { once: true });
  });
};

/**
 * Manages a single HTTP/2 session and request stream for a specific topic subscription.
 */
class GrpcTopicSession {
  constructor({
    grpcConfig,
    topic,
    schema,
    topicStreamRequestType,
    rawDataChunkType,
    requestPath,
    onMessage,
    signal,
    logger,
    label,
    onConnected,
  }) {
    this.grpcConfig = grpcConfig;
    this.topic = topic;
    this.schema = schema;
    this.topicStreamRequestType = topicStreamRequestType;
    this.rawDataChunkType = rawDataChunkType;
    this.requestPath = requestPath;
    this.onMessage = onMessage;
    this.signal = signal;
    this.logger = logger;
    this.label = label;
    this.onConnected = onConnected;

    this.session = null;
    this.stream = null;
    this.settled = false;
    this.grpcStatus = null;
    this.grpcMessage = null;
    this.handleAbort = this.handleAbort.bind(this);
  }

  async run() {
    if (this.signal.aborted) {
      return;
    }

    return new Promise((resolve, reject) => {
      this.resolve = resolve;
      this.reject = reject;

      this.signal.addEventListener("abort", this.handleAbort, { once: true });

      const authority = buildAuthority(this.grpcConfig);
      this.session = http2.connect(authority);

      // Handle session-level lifecycle
      this.session.once("error", (error) => this.settle(this.reject, error));
      this.session.once("close", () => {
        if (this.signal.aborted) {
          this.settle(this.resolve);
          return;
        }
        this.settle(
          this.reject,
          new Error(`grpc session closed before completion for topic ${this.topic}`)
        );
      });

      // Initiate request stream
      const requestHeaders = buildRequestHeaders(this.grpcConfig, this.requestPath);
      this.stream = this.session.request(requestHeaders);

      const parser = createGrpcFrameParser((messageBuffer) => this.handleMessageFrame(messageBuffer));

      // Handle stream response headers (checking status code)
      this.stream.once("response", (headers) => {
        const status = Number(headers[":status"]);
        if (status !== 200) {
          this.settle(this.reject, createSilentHttpStatusError(status, this.topic));
          this.destroy();
          return;
        }
        this.onConnected();
      });

      // Handle stream data
      this.stream.on("data", (chunk) => {
        try {
          parser.push(chunk);
        } catch (error) {
          this.settle(this.reject, error);
          this.destroy();
        }
      });

      // Handle trailers (extracting gRPC status & message)
      this.stream.once("trailers", (trailers) => {
        this.grpcStatus = trailers["grpc-status"];
        this.grpcMessage = trailers["grpc-message"];
      });

      // Handle stream error
      this.stream.once("error", (error) => {
        this.settle(this.reject, error);
        this.destroy();
      });

      // Handle stream end
      this.stream.once("end", () => {
        if (this.signal.aborted) {
          this.settle(this.resolve);
          return;
        }

        if (this.grpcStatus && this.grpcStatus !== "0") {
          this.settle(
            this.reject,
            createGrpcStatusError(this.grpcStatus, this.grpcMessage, this.topic)
          );
          this.destroy();
          return;
        }

        this.settle(this.resolve);
      });

      // Write and complete request payload
      const requestPayload = createRequestPayload(this.topicStreamRequestType, this.topic);
      this.stream.end(encodeGrpcMessage(requestPayload));
    });
  }

  handleMessageFrame(messageBuffer) {
    try {
      const rawChunk = this.rawDataChunkType.decode(messageBuffer);
      const chunkTopic = rawChunk.topic || this.topic;
      const payload = rawChunk.payload ? Buffer.from(rawChunk.payload) : Buffer.alloc(0);

      if (payload.length === 0) {
        this.logger.warn(`${this.label} received empty payload`, { topic: chunkTopic });
        return;
      }

      const decoded = decodePayload(this.schema, payload);
      this.onMessage({
        topic: chunkTopic,
        key: rawChunk.key || null,
        decoded,
        rawChunk,
        schema: this.schema,
      });
    } catch (error) {
      this.logger.error(`${this.label} failed to decode grpc payload`, {
        topic: this.topic,
        error: error.message,
      });
    }
  }

  handleAbort() {
    this.destroy();
    this.settle(this.resolve);
  }

  settle(action, value) {
    if (this.settled) {
      return;
    }
    this.settled = true;
    this.signal.removeEventListener("abort", this.handleAbort);
    action(value);
  }

  destroy() {
    if (this.stream) {
      try {
        this.stream.close(http2.constants.NGHTTP2_CANCEL);
      } catch {}
    }
    if (this.session) {
      try {
        this.session.destroy();
      } catch {}
    }
  }
}

/**
 * Manages the gRPC stream client for multiple topic subscriptions.
 */
class GrpcStreamClient {
  constructor({
    grpcConfig,
    streamName,
    label,
    topics,
    schema,
    topicStreamRequestType,
    rawDataChunkType,
    onMessage,
    requestPath = "/com.example.grpc.StreamRouter/SubscribeToTopic",
    logger,
  }) {
    this.grpcConfig = grpcConfig;
    this.streamName = streamName;
    this.label = label;
    this.topics = topics;
    this.schema = schema;
    this.topicStreamRequestType = topicStreamRequestType;
    this.rawDataChunkType = rawDataChunkType;
    this.onMessage = onMessage;
    this.requestPath = requestPath;
    this.logger = logger;

    this.abortController = new AbortController();
    this.topicStates = topics.map((topic) => ({
      topic,
      connected: false,
      hasEverConnected: false,
      lastError: null,
    }));
    this.started = false;
    this.loopPromises = [];
  }

  async start() {
    if (this.started) {
      return;
    }

    if (!this.topics.length) {
      throw new Error(`${this.streamName} requires at least one topic`);
    }

    this.started = true;
    this.logger.info(`${this.label} starting`, {
      authority: buildAuthority(this.grpcConfig),
      topics: this.topics,
    });

    this.loopPromises = this.topicStates.map((topicState) =>
      this.runTopicLoop(topicState)
    );
  }

  async stop() {
    this.abortController.abort();
    await Promise.allSettled(this.loopPromises);
  }

  getStatus() {
    return {
      started: this.started,
      connectedTopics: this.topicStates
        .filter((state) => state.connected)
        .map((state) => state.topic),
      lastError: this.topicStates.map((state) => state.lastError).find(Boolean) || null,
    };
  }

  async runTopicLoop(topicState) {
    const signal = this.abortController.signal;

    while (!signal.aborted) {
      try {
        const session = new GrpcTopicSession({
          grpcConfig: this.grpcConfig,
          topic: topicState.topic,
          schema: this.schema,
          topicStreamRequestType: this.topicStreamRequestType,
          rawDataChunkType: this.rawDataChunkType,
          requestPath: this.requestPath,
          onMessage: this.onMessage,
          signal,
          logger: this.logger,
          label: this.label,
          onConnected() {
            topicState.connected = true;
            topicState.hasEverConnected = true;
            topicState.lastError = null;
          },
        });

        await session.run();
      } catch (error) {
        topicState.connected = false;

        if (isTransientConnectionError(error)) {
          error.silent = true;
        }

        if (shouldSuppressStartupWaitError(error, topicState.hasEverConnected)) {
          topicState.lastError = null;
          continue;
        }

        if (!signal.aborted && !error.silent) {
          topicState.lastError = error.message;
          this.logger.error(`${this.label} subscription failed`, {
            topic: topicState.topic,
            error: error.message,
          });
        }
      }

      topicState.connected = false;

      if (!signal.aborted) {
        await delay(this.grpcConfig.reconnectDelayMs, signal);
      }
    }
  }
}

const createGrpcStreamClient = (options) => {
  return new GrpcStreamClient(options);
};

module.exports = {
  createGrpcStreamClient,
  shouldSuppressStartupWaitError,
};
