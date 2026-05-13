const PROTOBUF_TO_OBJECT_OPTIONS = Object.freeze({
  longs: String,
  enums: String,
  bytes: String,
  defaults: true,
  arrays: true,
  objects: true,
});

const TWIST_PUBLISH_OPTIONS = Object.freeze({
  persistent: false,
  deliveryMode: 1,
  contentType: "application/x-protobuf",
  headers: {
    "protobuf-type": "Twist",
  },
});

const MAP_CACHE_KEY = "map";

module.exports = {
  MAP_CACHE_KEY,
  PROTOBUF_TO_OBJECT_OPTIONS,
  TWIST_PUBLISH_OPTIONS,
};
