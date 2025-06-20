import { spawn } from "child_process";
import cron from "node-cron";
import redis from "./lib/redis";

let x = 0;

const posJob = cron.schedule("* * * * * *", async () => {
  console.log(`Current Observability on X -> ${x}`);
  x++;
  if (x % 5 === 0) {
    console.log("Running Worker");
    posJob.stop();

    // Observe Active update queues
    const active_update_queue_keys = await redis.keys("update_queue:*");
    if (!active_update_queue_keys || active_update_queue_keys.length === 0) {
      console.log(`No Active Update Queues observed`);
      posJob.start();
      return;
    }

    // For each active update queue key spawn in a worker
    active_update_queue_keys.forEach((queue_key) => {
      const posWorker = spawn(
        "ts-node",
        ["src/workers/worker-pos.ts", String(queue_key)],
        {
          shell: true,
        }
      );
      posWorker.stdout.on("data", (data) => {
        console.log(`POS-WORKER-${queue_key} : ${data}`);
      });

      posWorker.stderr.on("data", (data) => {
        console.error(`POS-WORKER-${queue_key} STDERR: ${data}`);
      });

      posWorker.on("close", (code) => {
        console.log(`POS-WORKER-${queue_key} exited with code ${code}`);
      });
    });

    // Start back observer on finishing all workers
    posJob.start();
  }
});
