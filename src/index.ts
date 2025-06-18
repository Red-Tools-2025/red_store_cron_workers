import { spawn } from "child_process";
import cron from "node-cron";

let x = 0;
let isWorkerRunning = false;

const posJob = cron.schedule("* * * * * *", () => {
  console.log(`Current Observability on X -> ${x}`);
  x++;
  if (x % 5 === 0) {
    // Declare a worker
    console.log("Running Worker");
    isWorkerRunning = true;

    posJob.stop();
    const posWorker = spawn("node", ["src/workers/worker-pos.ts"]);
    posWorker.stdout.on("data", (data) => {
      console.log(`POS-WORKER STDOUT : ${data}`);
    });

    posWorker.stderr.on("data", (data) => {
      console.error(`Worker STDERR: ${data}`);
    });

    posWorker.on("close", (code) => {
      console.log(`Worker exited with code ${code}`);
      isWorkerRunning = false;
      posJob.start();
    });
  }
});
