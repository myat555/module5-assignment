# shared RabbitMQ helpers + settings used by the producer and workers
import logging
import os
import time

import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
QUEUE_NAME = os.getenv("ORDER_QUEUE", "orders")

def setup_logging(name: str) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("pika").setLevel(logging.WARNING)
    return logging.getLogger(name)

def get_connection(log: logging.Logger, retries: int = 10) -> pika.BlockingConnection:
    """Connect to RabbitMQ, retrying while the broker is still starting up."""
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS),
        heartbeat=30,
    )
    for attempt in range(1, retries + 1):
        try:
            return pika.BlockingConnection(params)
        except pika.exceptions.AMQPConnectionError:
            log.warning("RabbitMQ not reachable (attempt %d/%d), retrying in 2s...", attempt, retries)
            time.sleep(2)
    raise SystemExit("Could not connect to RabbitMQ")

def declare_queue(channel) -> None:
    # durable=True -> the queue survives a broker restart
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
