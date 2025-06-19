import { type SaleEvent } from "../types/sales";

const isValidSaleEvent = (obj: any): obj is SaleEvent => {
  if (
    obj &&
    typeof obj === "object" &&
    typeof obj.p_id === "number" &&
    typeof obj.delta === "number" &&
    typeof obj.timestamp === "number" &&
    (typeof obj.storeId === "number" || typeof obj.store_id === "number")
  ) {
    // Normalize snake_case → camelCase
    if (typeof obj.store_id === "number" && typeof obj.storeId !== "number") {
      obj.storeId = obj.store_id;
    }
    return true;
  }

  return false;
};

export { isValidSaleEvent };
