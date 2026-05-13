require("dotenv").config();
const { MongoClient } = require("mongodb");

const MONGO_URI = process.env.MONGO_URI || "mongodb://10.0.0.6:27017/sevenhub";
const DB_NAME = process.env.DB_NAME || "sevenhub";

async function run() {
  const client = new MongoClient(MONGO_URI);
  try {
    await client.connect();
    const db = client.db(DB_NAME);
    const collections = await db.listCollections().toArray();
    console.log("Collections:", collections.map((collection) => collection.name));
    for (const c of collections) {
      const count = await db.collection(c.name).countDocuments();
      console.log(`Collection ${c.name} count: ${count}`);
    }
  } finally {
    await client.close();
  }
}

run();
