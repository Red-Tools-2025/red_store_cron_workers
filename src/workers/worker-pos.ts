import { SaleEvent } from "../types/sales";

import { pool } from "../lib/db";
import redis from "../lib/redis";

import { isValidSaleEvent } from "../utils/typeGaurds";

console.log("worker-pos initiated");
const redisCheck = async () => {
  console.log("Running Pipeline");
  try {
    const redisQL = await redis.llen("update_queue:1");
    const redisQD = await redis.lrange("update_queue:1", 0, -1);

    if (redisQD.length === 0) {
      console.log(`Aborting Worker Job, No Updates for sales`);
      return null;
    }

    // Protection and verification against malformed or corrupted strings
    const parsedRedisQD: SaleEvent[] = redisQD
      .map((entry) => {
        try {
          const parsedEntry = JSON.parse(entry);
          if (isValidSaleEvent(parsedEntry)) {
            return parsedEntry;
          } else {
            throw new Error(
              `Malformed Entry detected did not pass type gaurd check: ${entry}`
            );
          }
        } catch (e) {
          console.log(
            `Ditected Malfomed or illegal cache entry : ${entry}, error details : ${e}`
          );
          return null;
        }
      })
      .filter((event): event is SaleEvent => event !== null);

    // Map Sales ID
    const saleProductIds = parsedRedisQD.map((event) => event.p_id);

    // Define placefolder for filtering on Query level
    const placeholders = saleProductIds.map((_, i) => `$${i + 1}`).join(", ");
    const query = `SELECT * FROM inventory WHERE store_id = 1 AND id IN (${placeholders})`;

    // Fetch relevant products
    const db_current_products = await pool.query(query, saleProductIds);
    const rows = db_current_products.rows;

    // Filter out products with associated sales events
    console.log({
      update_queue_len: redisQL,
      update_queue_data: redisQD,
      current_products: rows,
    });
    console.log("Finished Pipeline");
  } catch (e) {
    console.error("Error while running pipeline", e);
  }
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
