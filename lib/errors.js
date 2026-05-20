class AppError extends Error {
  constructor(message, options = {}) {
    super(message);
    this.name = this.constructor.name;
    this.code = options.code || "APP_ERROR";
    this.statusCode = options.statusCode || 500;
    this.details = options.details;
  }
}

class ValidationError extends AppError {
  constructor(message, details) {
    super(message, {
      code: "VALIDATION_ERROR",
      statusCode: 400,
      details,
    });
  }
}

class DependencyUnavailableError extends AppError {
  constructor(message, details) {
    super(message, {
      code: "DEPENDENCY_UNAVAILABLE",
      statusCode: 503,
      details,
    });
  }
}

class NotFoundError extends AppError {
  constructor(message, details) {
    super(message, {
      code: "NOT_FOUND",
      statusCode: 404,
      details,
    });
  }
}

const sendErrorResponse = (res, error, logger) => {
  if (error instanceof AppError) {
    if (logger && error.statusCode >= 500) {
      logger.error("Application error", error.message, error.details || "");
    }

    return res.status(error.statusCode).json({
      error: error.message,
      details: error.details,
    });
  }

  if (logger) {
    logger.error("Unhandled error", error);
  }

  return res.status(500).json({
    error: "Internal server error",
    details: error.message,
  });
};

module.exports = {
  AppError,
  DependencyUnavailableError,
  NotFoundError,
  ValidationError,
  sendErrorResponse,
};
