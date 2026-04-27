const { MongoClient } = require('mongodb');
const MONGO_URI = 'mongodb://10.0.0.6:27017/sevenhub';
async function run() {
  const client = new MongoClient(MONGO_URI);
  try {
    await client.connect();
    const db = client.db('sevenhub');
    const waypoints = await db.collection('waypoints').find({}).toArray();
    console.log('Waypoints found in DB:', JSON.stringify(waypoints, null, 2));
  } finally {
    await client.close();
  }
}
run();
