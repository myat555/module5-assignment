# Order Worker: consumes order messages, processes them, then ACKs.

import argparse
import json
import os
import time
from contextlib import contextmanager

from common import QUEUE_NAME, declare_queue, get_connection, setup_logging

PROCESSED_FILE = os.getenv("PROCESSED_FILE", "processed_events.txt")


class ProcessedEventStore:
    # Tiny file-backed set of processed event_ids, shared by all worker processes.
   
    def __init__(self, path: str):
        self.path = path
        self.lock_path = path + ".lock"
        open(self.path, "a").close()
 
    @contextmanager
    def _locked(self):
        with open(self.lock_path, "a+") as lock_file:
            if os.name == "nt":
                import msvcrt
                lock_file.seek(0)
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)
                try:
                    yield
                finally:
                    lock_file.seek(0)
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(lock_file, fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(lock_file, fcntl.LOCK_UN)
 
    def contains(self, event_id: str) -> bool:
        with self._locked(), open(self.path) as f:
            return event_id in {line.strip() for line in f}
 
    def add(self, event_id: str) -> None:
        with self._locked(), open(self.path, "a") as f:
            f.write(event_id + "\n")
            f.flush()
            os.fsync(f.fileno())
 
    def reset(self) -> None:
        with self._locked():
            open(self.path, "w").close()
 
 
def main() -> None:
    parser = argparse.ArgumentParser(description="Consume and process order messages")
    parser.add_argument("--name", default=f"worker-{os.getpid()}", help="worker name shown in logs")
    parser.add_argument("--work-seconds", type=float, default=2.0, help="simulated processing time")
    parser.add_argument("--crash-on", metavar="ORDER_ID", help="simulate a crash when this order_id arrives")
    parser.add_argument("--crash-on-nth", type=int, metavar="N",
                        help="simulate a crash on the Nth new message THIS worker receives "
                             "(works regardless of which orders RabbitMQ dispatches to it)")
    parser.add_argument("--crash-point", choices=["before-process", "after-process"], default="before-process")
    parser.add_argument("--reset", action="store_true", help="clear the processed event_id file on start")
    args = parser.parse_args()
 
    log = setup_logging(args.name)
    store = ProcessedEventStore(PROCESSED_FILE)
    if args.reset:
        store.reset()
        log.info("Cleared %s", PROCESSED_FILE)
 
    connection = get_connection(log)
    channel = connection.channel()
    declare_queue(channel)
    # prefetch_count=1: RabbitMQ gives this worker one unacked message at a time,
    # so messages are spread fairly across competing workers.
    channel.basic_qos(prefetch_count=1)
 
    def simulate_crash(where: str) -> None:
        log.error("SIMULATED CRASH %s - exiting WITHOUT sending ACK", where)
        os._exit(1)  # hard exit: no ACK, connection drops, broker requeues the message
 
    received_count = [0]  # new (non-duplicate) messages this worker has received
 
    def on_message(ch, method, properties, body):
        order = json.loads(body)
        event_id, order_id = order["event_id"], order["order_id"]
        log.info("RECEIVED order_id=%s event_id=%s redelivered=%s delivery_tag=%s",
                 order_id, event_id, method.redelivered, method.delivery_tag)
 
        # 1. Idempotency check
        if store.contains(event_id):
            log.warning("DUPLICATE detected event_id=%s order_id=%s - business action skipped",
                        event_id, order_id)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            log.info("ACK sent for duplicate order_id=%s", order_id)
            return
 
        received_count[0] += 1
        crash_here = (args.crash_on == order_id) or (args.crash_on_nth == received_count[0])
        if crash_here and args.crash_point == "before-process":
            simulate_crash(f"before processing {order_id}")
 
        # 2. Business action (simulated)
        log.info("PROCESSING order_id=%s item_id=%s quantity=%s",
                 order_id, order["item_id"], order["quantity"])
        time.sleep(args.work_seconds)
 
        # 3. Record the event as processed, then ACK
        store.add(event_id)
        log.info("DONE order_id=%s (event_id recorded)", order_id)
 
        if crash_here and args.crash_point == "after-process":
            simulate_crash(f"after processing {order_id}")
 
        ch.basic_ack(delivery_tag=method.delivery_tag)
        log.info("ACK sent for order_id=%s", order_id)
 
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message, auto_ack=False)
    log.info("Waiting for messages on queue '%s' (Ctrl+C to stop)", QUEUE_NAME)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        log.info("Stopping worker")
        channel.stop_consuming()
        connection.close()
 
 
if __name__ == "__main__":
    main()
 
