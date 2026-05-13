require("dotenv").config();
const { MongoClient } = require("mongodb");

const MONGO_URI = process.env.MONGO_URI || "mongodb://10.0.0.6:27017/sevenhub";
const DB_NAME = process.env.DB_NAME || "sevenhub";
const WAYPOINT_COLLECTION = process.env.WAYPOINT_COLLECTION || "waypoints";

async function run() {
  const client = new MongoClient(MONGO_URI);
  try {
    await client.connect();
    const db = client.db(DB_NAME);
    const waypoints = await db.collection(WAYPOINT_COLLECTION).find({}).toArray();
    console.log("Waypoints found in DB:", JSON.stringify(waypoints, null, 2));
  } finally {
    await client.close();
  }
}

run();
