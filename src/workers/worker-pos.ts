import { redis } from "../lib/redis";

console.log("worker-pos says hi");
setTimeout(() => {
  console.log("Worker finished after 2 seconds");
}, 2000);
