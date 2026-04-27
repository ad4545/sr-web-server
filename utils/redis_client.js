const Redis = require("ioredis");
// const logger = require("./logger");
require("dotenv").config();

const redis_client = new Redis({
  host: process.env.REDIS_HOST,  // Change to your Redis host (or ElastiCache endpoint)
  port: process.env.REDIS_PORT,         // Default Redis port
  retryStrategy(times) {
    return Math.min(times * 50, 2000); // Retry with delay
  },
});



redis_client.on("error", (err) => console.log(`Redis Client Error: ${err.message}`));



module.exports = {redis_client}
