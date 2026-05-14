function encodeGrpcMessage(payload) {
  const body = Buffer.isBuffer(payload) ? payload : Buffer.from(payload);
  const frame = Buffer.allocUnsafe(5 + body.length);

  frame.writeUInt8(0, 0);
  frame.writeUInt32BE(body.length, 1);
  body.copy(frame, 5);

  return frame;
}

function createGrpcFrameParser(onMessage) {
  let buffer = Buffer.alloc(0);

  return {
    push(chunk) {
      buffer = Buffer.concat([buffer, Buffer.from(chunk)]);

      while (buffer.length >= 5) {
        const compressed = buffer.readUInt8(0);
        if (compressed !== 0) {
          throw new Error("Compressed gRPC messages are not supported.");
        }

        const messageLength = buffer.readUInt32BE(1);
        if (buffer.length < 5 + messageLength) {
          return;
        }

        const message = buffer.subarray(5, 5 + messageLength);
        onMessage(message);
        buffer = buffer.subarray(5 + messageLength);
      }
    },
  };
}

module.exports = {
  createGrpcFrameParser,
  encodeGrpcMessage,
};
