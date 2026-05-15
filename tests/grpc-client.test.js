const test = require("node:test");
const assert = require("node:assert/strict");

const { createGrpcFrameParser, encodeGrpcMessage } = require("../realtime/grpc/framing");
const { loadOdometryType, loadStreamRouterTypes } = require("../clients/protobuf");

test("grpc framing helpers encode and reassemble messages", () => {
  const payload = Buffer.from("hello-grpc");
  const frame = encodeGrpcMessage(payload);

  assert.equal(frame.readUInt8(0), 0);
  assert.equal(frame.readUInt32BE(1), payload.length);

  const decoded = [];
  const parser = createGrpcFrameParser((message) => {
    decoded.push(message);
  });

  parser.push(frame.subarray(0, 3));
  parser.push(frame.subarray(3));

  assert.equal(decoded.length, 1);
  assert.deepEqual(decoded[0], payload);
});

test("grpc protobuf loaders resolve the stream router and odometry messages", async () => {
  const [odometryType, streamRouterTypes] = await Promise.all([
    loadOdometryType(),
    loadStreamRouterTypes(),
  ]);

  assert.equal(odometryType.fullName, ".combined_odom.Odometry");
  assert.equal(streamRouterTypes.topicStreamRequestType.fullName, ".com.example.grpc.TopicStreamRequest");
  assert.equal(streamRouterTypes.rawDataChunkType.fullName, ".com.example.grpc.RawDataChunk");
});
