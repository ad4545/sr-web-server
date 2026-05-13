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

module.exports = {
  AppError,
  DependencyUnavailableError,
  NotFoundError,
  ValidationError,
};
