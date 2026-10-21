# Order Producer: publishes order messages to the RabbitMQ orders queue
import argparse
import json
import random
import uuid

import pika

from common import QUEUE_NAME, declare_queue, get_connection, setup_logging

log = setup_logging("producer")

def build_order(order_number : int) -> dict:
    return {
        "event_id": str(uuid.uuid4()), # unique per event -> for idempotency
        "order_id": f"ORD-{order_number}",
        "item_id": f"ITEM-{random.randInt(100, 999)}",
        "quantity": random.randInt(1, 5),
    }

def publish(channel, order: dict) -> None:
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_NAME,
        body=json.dumps(order),
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=pika.DeliveryMode.Persistent,
            message_id=order["event_id"],
        ),
    )
    log.info("PUBLISHED %s", json.dumps(order))

def main() -> None:
    parser = argparse.ArgumentParser(description="Publish order messages")
    parser.add_argument("--count", type=int, default=6, help="number of orders to publish")
    parser.add_argument("--start", type=int, default=1001, help="first order number (ORD-<start>)")
    parser.add_argument("--duplicate", action="store_true",
                        help="re-publish the first order again with the same event_id")
    args = parser.parse_args()
 
    connection = get_connection(log)
    channel = connection.channel()
    declare_queue(channel)
    channel.confirm_delivery()  # broker confirms every publish
 
    orders = [build_order(args.start + i) for i in range(args.count)]
    for order in orders:
        publish(channel, order)
 
    if args.duplicate and orders:
        log.info("Re-publishing first order as a DUPLICATE (same event_id=%s)", orders[0]["event_id"])
        publish(channel, orders[0])
 
    connection.close()
    log.info("Done. %d message(s) sent to queue '%s'.", args.count + (1 if args.duplicate else 0), QUEUE_NAME)
 
 
if __name__ == "__main__":
    main()
 
