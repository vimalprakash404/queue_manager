require('dotenv').config();
const Redis = require('ioredis');
const { MongoClient } = require('mongodb');

// Load env variables
const {
  REDIS_HOST,
  REDIS_PORT,
  REDIS_USERNAME,
  REDIS_PASSWORD,
  REDIS_QUEUE_NAME,
  MONGOURI,
  MONGO_DB_NAME,
  MONGO_COLLECTION_NAME
} = process.env;

// Redis client
const redis = new Redis({
  host: REDIS_HOST,
  port: REDIS_PORT,
  username: REDIS_USERNAME || undefined,
  password: REDIS_PASSWORD || undefined,
});

// Mongo client
const mongoClient = new MongoClient(MONGOURI);

async function connectMongo() {
  await mongoClient.connect();
  const db = mongoClient.db(MONGO_DB_NAME);
  const collection = db.collection(MONGO_COLLECTION_NAME);
  return collection;
}

// Utility to clean object (convert NaN to null)
function cleanObject(obj) {
  return JSON.parse(
    JSON.stringify(obj, (key, value) =>
      typeof value === 'number' && isNaN(value) ? null : value
    )
  );
}

// Main job processor
async function run() {
    const collection = await connectMongo();
    console.log('Connected to MongoDB and Redis');
  
    while (true) {
      const item = await redis.lpop(REDIS_QUEUE_NAME);
      if (!item) {
        console.log('Queue empty. Waiting...');
        await new Promise((res) => setTimeout(res, 3000));
        continue;
      }
  
      try {
        // Clean string: replace NaN with null before parsing
        const sanitized = item.replace(/\bNaN\b/g, 'null');
        const raw = JSON.parse(sanitized);
  
        const parsed = cleanObject(raw);
        await collection.insertOne(parsed);
        console.log(`Inserted: ${parsed.id}`);
      } catch (err) {
        console.error('Insert failed or invalid JSON:', err);
      }
    }
  }
  

run().catch(console.error);
