const test = require("node:test");
const assert = require("node:assert/strict");

const { createTwistHandler } = require("../../realtime/handlers/twist");

function createLogger() {
  return {
    warn() {},
    error() {},
  };
}

test("twist handler acknowledges valid payloads", async () => {
  const published = [];
  const acknowledgements = [];
  const twistHandler = createTwistHandler({
    rabbitMqClient: {
      isReady() {
        return true;
      },
      getLastError() {
        return null;
      },
      async publish(message) {
        published.push(message);
      },
      async close() {},
    },
    rabbitConfig: {
      exchange: "client",
      routingKey: "joystick",
    },
    twistType: {
      verify() {
        return null;
      },
      create(payload) {
        return payload;
      },
      encode(payload) {
        return {
          finish() {
            return Buffer.from(JSON.stringify(payload));
          },
        };
      },
    },
    logger: createLogger(),
  });

  await twistHandler(
    {
      Linear: { X: 1, Y: 0, Z: 0 },
      Angular: { X: 0, Y: 0, Z: 1 },
    },
    (payload) => {
      acknowledgements.push(payload);
    }
  );

  assert.equal(published.length, 1);
  assert.deepEqual(acknowledgements, [{ ok: true }]);
});

test("twist handler rejects malformed payloads without publishing", async () => {
  const acknowledgements = [];
  const twistHandler = createTwistHandler({
    rabbitMqClient: {
      isReady() {
        return true;
      },
      getLastError() {
        return null;
      },
      async publish() {
        assert.fail("publish should not be called for invalid twist");
      },
      async close() {},
    },
    rabbitConfig: {
      exchange: "client",
      routingKey: "joystick",
    },
    twistType: {
      verify() {
        return null;
      },
      create(payload) {
        return payload;
      },
      encode(payload) {
        return {
          finish() {
            return Buffer.from(JSON.stringify(payload));
          },
        };
      },
    },
    logger: createLogger(),
  });

  await twistHandler(
    {
      Linear: { X: 1, Y: 0, Z: 0 },
    },
    (payload) => {
      acknowledgements.push(payload);
    }
  );

  assert.deepEqual(acknowledgements, [
    {
      ok: false,
      error: "Angular is required.",
    },
  ]);
});

test("twist handler returns failure ack when publish fails", async () => {
  const acknowledgements = [];
  const twistHandler = createTwistHandler({
    rabbitMqClient: {
      isReady() {
        return false;
      },
      getLastError() {
        return new Error("auth failed");
      },
      async publish() {
        throw new Error("publish failed");
      },
      async close() {},
    },
    rabbitConfig: {
      exchange: "client",
      routingKey: "joystick",
    },
    twistType: {
      verify() {
        return null;
      },
      create(payload) {
        return payload;
      },
      encode(payload) {
        return {
          finish() {
            return Buffer.from(JSON.stringify(payload));
          },
        };
      },
    },
    logger: createLogger(),
  });

  await twistHandler(
    {
      Linear: { X: 1, Y: 0, Z: 0 },
      Angular: { X: 0, Y: 0, Z: 1 },
    },
    (payload) => {
      acknowledgements.push(payload);
    }
  );

  assert.deepEqual(acknowledgements, [
    {
      ok: false,
      error: "Failed to publish twist message.",
    },
  ]);
});
