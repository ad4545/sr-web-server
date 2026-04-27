const AWS = require('aws-sdk');
const { redis_client } = require("../utils/redis_client");
require("dotenv").config();

const s3 = new AWS.S3({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: "ap-south-2",
});

const bucketName = process.env.S3_BUCKET;
const key = process.env.MAP_NAME;

const getMap = async (req, res) => {
  console.log("get map called ------------------------");
  try {
    // Check Redis cache first
    let map = await redis_client.get("map");
    if (map) {
      console.log("Serving from Redis cache");
      const buffer = Buffer.from(map, "base64");
      return res.status(200).send(buffer);
    }

    // Fetch directly from S3
    console.log("Fetching from S3...");
    const data = await s3.getObject({ Bucket: bucketName, Key: key }).promise();
    const fileBuffer = data.Body;

    // Cache it in Redis
    await redis_client.set("map", fileBuffer.toString("base64"));

    return res.status(200).send(fileBuffer);
  } catch (err) {
    console.error("Error in getMap:", err);
    return res.status(500).json({ error: err.message });
  }
};

module.exports = getMap;