import logging
import logging.config
import json
from configparser import ConfigParser

from confluent_kafka import Consumer


def run_kafka_consumer(config: ConfigParser) -> Consumer:
    """
    tbd
    """
    consumer = Consumer({
        "bootstrap.servers": config.get("kafka", "bootstrap_servers"),
        "group.id": config.get("kafka", "group_id"),
        "auto.offset.reset": config.get("kafka", "auto_offset_reset")
    })

    # subscribe to topic
    consumer.subscribe(topics=[config.get("kafka", "topic")])

    return consumer


if __name__ == "__main__":

    # load config
    config = ConfigParser()
    config.read("app.ini")

    # start logging
    logging.config.fileConfig("logging.ini")
    logger = logging.getLogger(__name__)
    logger.info("Starting")

    # start kafka consumer and subscribe to topic
    logger.info("Starting Kafka Consumer")
    consumer = run_kafka_consumer(config)

    # consume messages
    while True:
        msg = consumer.poll(timeout=.0)

        if msg is None:
            logging.debug("No message received")
            continue
        elif msg.error():
            logging.error(f"Consumer error: {msg.error()}")
            continue
        else:
            logging.info(f"Received message: {msg.value().decode('utf-8')}")

    consumer.close()