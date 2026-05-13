const { AppError } = require("../lib/errors");

function createErrorHandler(logger) {
  return function errorHandler(error, req, res, next) {
    if (res.headersSent) {
      return next(error);
    }

    if (error instanceof AppError) {
      if (error.statusCode >= 500) {
        logger.error("Application error", error.message, error.details || "");
      }

      return res.status(error.statusCode).json({
        error: error.message,
        details: error.details,
      });
    }

    logger.error("Unhandled error", error);
    return res.status(500).json({
      error: "Internal server error",
      details: error.message,
    });
  };
}

module.exports = {
  createErrorHandler,
};
