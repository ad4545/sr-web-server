const STARTUP_WAIT_ERROR_CODES = new Set(["ECONNREFUSED", "ECONNRESET", "EPIPE", "ENOTFOUND"]);
const STARTUP_WAIT_ERROR_PATTERNS = [
  /ECONNREFUSED/,
  /ECONNRESET/,
  /EPIPE/,
  /ENOTFOUND/,
  /grpc session closed before completion/i,
];

/**
 * Creates an error for HTTP statuses other than 200.
 * Status 404 is considered silent (no log warning).
 */
const createSilentHttpStatusError = (status, topic) => {
  const error = new Error(`unexpected grpc http status ${status} for topic ${topic}`);
  error.silent = status === 404;
  return error;
};

/**
 * Creates an error representing a gRPC status code failure from trailers.
 * Status 5 (NOT_FOUND) is considered silent.
 */
const createGrpcStatusError = (status, message, topic) => {
  const error = new Error(
    `grpc stream ended with status ${status} for topic ${topic}${message ? `: ${message}` : ""}`
  );
  error.grpcStatus = status;
  error.grpcTopic = topic;
  error.silent = status === "5";
  error.recoverable = true;
  return error;
};

/**
 * Checks if the connection error is a transient network event.
 */
const isTransientConnectionError = (error) => {
  if (!error) {
    return false;
  }

  if (typeof error.code === "string" && STARTUP_WAIT_ERROR_CODES.has(error.code)) {
    return true;
  }

  const message = typeof error.message === "string" ? error.message : "";
  return STARTUP_WAIT_ERROR_PATTERNS.some((pattern) => pattern.test(message));
};

/**
 * Checks if the connection error is a transient startup wait error that can be suppressed.
 */
const shouldSuppressStartupWaitError = (error, hasEverConnected) => {
  if (hasEverConnected) {
    return false;
  }

  return isTransientConnectionError(error);
};

module.exports = {
  createSilentHttpStatusError,
  createGrpcStatusError,
  isTransientConnectionError,
  shouldSuppressStartupWaitError,
};
