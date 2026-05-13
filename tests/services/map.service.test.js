const test = require("node:test");
const assert = require("node:assert/strict");

const { createMapHandler } = require("../../api/handlers/map");

function createLogger() {
  return {
    info() {},
  };
}

test("map handler returns cached map buffer when cache hit exists", async () => {
  const sourceBuffer = Buffer.from("cached-map");
  const mapHandler = createMapHandler({
    redisClient: {
      async get(key) {
        assert.equal(key, "map");
        return sourceBuffer.toString("base64");
      },
      async set() {
        assert.fail("cache should not be updated on hit");
      },
    },
    s3: {
      getObject() {
        assert.fail("asset store should not be called on hit");
        return {
          promise: async () => ({}),
        };
      },
    },
    bucketName: "bucket",
    key: "map",
    logger: createLogger(),
  });

  const res = {
    status(code) {
      assert.equal(code, 200);
      return this;
    },
    send(buffer) {
      assert.deepEqual(buffer, sourceBuffer);
    },
  };

  await mapHandler.getMap({}, res);
});

test("map handler fetches and caches map buffer on cache miss", async () => {
  const sourceBuffer = Buffer.from("fresh-map");
  const writes = [];
  const mapHandler = createMapHandler({
    redisClient: {
      async get() {
        return null;
      },
      async set(key, value) {
        writes.push({ key, value });
      },
    },
    s3: {
      getObject() {
        return {
          promise: async () => ({
            Body: sourceBuffer,
          }),
        };
      },
    },
    bucketName: "bucket",
    key: "map",
    logger: createLogger(),
  });

  const res = {
    status(code) {
      assert.equal(code, 200);
      return this;
    },
    send(buffer) {
      assert.deepEqual(buffer, sourceBuffer);
    },
  };

  await mapHandler.getMap({}, res);

  assert.deepEqual(writes, [
    {
      key: "map",
      value: sourceBuffer.toString("base64"),
    },
  ]);
});
