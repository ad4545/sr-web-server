const entrypoints = {
  "api-core": "./api/server",
  "realtime-core": "./realtime/server",
};

const role = process.env.APP_ROLE;

if (!role || !entrypoints[role]) {
  console.error(
    "APP_ROLE must be set to one of: api-core, realtime-core."
  );
  process.exit(1);
}

require(entrypoints[role]);
