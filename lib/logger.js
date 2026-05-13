function formatArgs(args) {
  return args.map((value) => {
    if (value instanceof Error) {
      return value.stack || value.message;
    }

    if (typeof value === "object" && value !== null) {
      try {
        return JSON.stringify(value);
      } catch (error) {
        return "[unserializable-object]";
      }
    }

    return value;
  });
}

function createLogger(scope = "app") {
  function log(method, args) {
    const stamp = new Date().toISOString();
    console[method](`[${stamp}] [${scope}]`, ...formatArgs(args));
  }

  return {
    debug: (...args) => log("debug", args),
    info: (...args) => log("log", args),
    warn: (...args) => log("warn", args),
    error: (...args) => log("error", args),
    child(childScope) {
      return createLogger(`${scope}:${childScope}`);
    },
  };
}

module.exports = {
  createLogger,
};
