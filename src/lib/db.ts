import { Pool } from "pg";
import dotenv from "dotenv";

dotenv.config();

const getPgConnectionString = () => {
  if (process.env.DATABASE_URL) {
    return process.env.DATABASE_URL;
  }
  throw new Error("DATABASE_URL env is not defined");
};
const pool = new Pool({
  connectionString: getPgConnectionString(),
});

export { pool };
