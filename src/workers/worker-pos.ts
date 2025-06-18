import redis from "../lib/redis";

console.log("worker-pos initiated");
const redisCheck = async () => {
  console.log("Running Pipeline");
  const redisRead = await redis.smembers("inv_products:1");
  console.log(redisRead);
  console.log("Finished Pipeline");
};

// RunPromise
redisCheck()
  .then(() => {
    console.log("Finished Redis Check");
    process.exit(0);
  })
  .catch((err) => {
    console.log(`Error During Redis Check ${err}`);
  });
