import { SaleEvent } from "../types/sales";

import { pool } from "../lib/db";
import redis from "../lib/redis";

import { isValidSaleEvent } from "../utils/typeGaurds";

const queue_key = process.argv[2];
console.log(`worker-pos initiated for queue_key:${queue_key}`);
const redisCheck = async () => {
  console.log("Running Pipeline");
  try {
    const redisQD = await redis.lrange(queue_key, 0, -1);

    // This is TX Marking Log that will keep track of failed transactions and prevent removal of log from redis cache for a second attempt next time
    let redisQDLogStatus: {
      saleEvent: SaleEvent;
      tx_status: boolean;
      raw_key: string;
    }[] = [];

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

    // Populate TX mark Log
    redisQDLogStatus = parsedRedisQD.map((e, idx) => {
      return { saleEvent: e, tx_status: false, raw_key: redisQD[idx] };
    });

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
    let transaction_count = 0;
    await pool.query("BEGIN");
    for (const [key, totalDelta] of queueMap) {
      // Split key to get entry attributes
      const [p_id, storeId] = key.split("_").map(Number);
      try {
        const result = await pool.query(
          `UPDATE inventory
           SET quantity = quantity + $1
           WHERE id = $2 AND store_id = $3`,
          [totalDelta, p_id, storeId]
        );

        if (result.rowCount === 0) {
          console.warn(
            `No inventory row found for p_id=${p_id}, storeId=${storeId}`
          );
        }

        // Mark all logs with matching p_id and storeId as processed
        redisQDLogStatus.forEach((log) => {
          if (
            log.saleEvent.p_id === p_id &&
            log.saleEvent.storeId === storeId
          ) {
            log.tx_status = true;
          }
        });

        console.log(
          `Marked TX for store_id:${storeId} inventory count for product:${p_id}`
        );
        transaction_count++;
      } catch (updateError) {
        console.error(
          `Failed to update inventory for p_id=${p_id}, error:`,
          updateError
        );
      }
    }
    await pool.query("COMMIT");
    console.log(`Transaction count ${transaction_count}`);

    // Filter out products with associated sales events

    // Clear out processed sales logs
    const logClearPipeline = redis.pipeline();

    for (const log of redisQDLogStatus) {
      if (log.tx_status) {
        logClearPipeline.lrem(queue_key, 1, log.raw_key);
      }
    }

    const results = await logClearPipeline.exec();
    console.log(`Removed ${results?.length} processed sale logs from Redis.`);

    redisQDLogStatus.forEach((tx_log) => {
      if (tx_log.tx_status === false) {
        console.log(
          `Failed TX for event:`,
          tx_log.saleEvent,
          `, will be scheduled for next task`
        );
      }
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
