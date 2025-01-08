import { ethereum } from "@graphprotocol/graph-ts";
import { Transaction } from "../helper/constants";

export function createEventID(event: ethereum.Event, transaction: Transaction): string {
  return event.transaction.hash
    .concatI32(event.logIndex.toI32())
    .concatI32(transaction).toHexString()
}
