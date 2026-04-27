const { MongoClient } = require('mongodb');
const MONGO_URI = 'mongodb://10.0.0.6:27017/sevenhub';
async function run() {
  const client = new MongoClient(MONGO_URI);
  try {
    await client.connect();
    const db = client.db('sevenhub');
    const collections = await db.listCollections().toArray();
    console.log('Collections:', collections.map(c => c.name));
    for (const c of collections) {
        const count = await db.collection(c.name).countDocuments();
        console.log(`Collection ${c.name} count: ${count}`);
    }
  } finally {
    await client.close();
  }
}
run();
