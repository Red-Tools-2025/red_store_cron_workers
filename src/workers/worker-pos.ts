import { pool } from "../lib/db";
import redis from "../lib/redis";

console.log("worker-pos initiated");
const redisCheck = async () => {
  console.log("Running Pipeline");
  const redisQL = await redis.llen("update_queue:1");
  const redisQD = await redis.lrange("update_queue:1", 0, -1);

  const db_current_products = await pool.query(
    "SELECT * FROM inventory WHERE store_id=1"
  );
  const rows = db_current_products.rows;
  console.log({
    update_queue_len: redisQL,
    update_queue_data: redisQD,
    current_products: rows,
  });
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
