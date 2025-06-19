import { type SaleEvent } from "../types/sales";

const isValidSaleEvent = (obj: any): obj is SaleEvent => {
  return (
    obj &&
    typeof obj === "object" &&
    typeof obj.p_id === "number" &&
    typeof obj.delta === "number" &&
    typeof obj.timestamp === "number" &&
    typeof obj.storeId === "number"
  );
};

export { isValidSaleEvent };
