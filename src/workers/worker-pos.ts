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
      return;
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

    console.log(
      `Validated Cache logs, Processing ${parsedRedisQD.length} sale events.`
    );

    // Updates Queue is processed separately as opposed to sales queue so we can group up similar IDs to reduce transaction load on DB
    // A parsed map accumaltes the delta of those cache logs that belong to the same p_id to reduce transaction load
    const queueMap = new Map<string, number>();

    for (const updateLog of parsedRedisQD) {
      const mapKey = `${updateLog.p_id}_${updateLog.storeId}`;
      queueMap.set(
        mapKey,
        // Accumalate previous entries, if entered before
        (queueMap.get(mapKey) || 0) + updateLog.delta
      );
    }

    //Wrapping all QTY updates in a single transaction
    await pool.query("BEGIN");
    for (const [key, totalDelta] of queueMap) {
      // Split key to get entry attributes
      const [p_id, storeId] = key.split("_").map(Number);
      try {
        const result = await pool.query(
          `UPDATE inventory
           SET quantity = quantity + $1
           WHERE p_id = $2 AND store_id = $3`,
          [totalDelta, p_id, storeId]
        );

        if (result.rowCount === 0) {
          console.warn(
            `No inventory row found for p_id=${p_id}, storeId=${storeId}`
          );
        }
      } catch (updateError) {
        console.error(
          `Failed to update inventory for p_id=${p_id}, error:`,
          updateError
        );
      }
    }
    await pool.query("COMMIT");

    // Filter out products with associated sales events
    console.log({
      update_queue_len: redisQL,
      update_queue_data: redisQD,
    });

    // Clear out processed sales logs
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
