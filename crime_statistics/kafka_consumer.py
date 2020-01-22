import logging
import logging.config
from configparser import ConfigParser

from confluent_kafka import Consumer


def run_kafka_consumer():
    """
    Create Kafka consumer, subscribe to relevant topic and start consuming messages
    """

    # load config
    config = ConfigParser()
    config.read("app.cfg")

    # start kafka consumer
    logger.info("Starting Kafka Consumer")
    consumer = Consumer({
        "bootstrap.servers": config.get("kafka", "bootstrap_servers"),
        "group.id": config.get("kafka", "group_id"),
        "auto.offset.reset": config.get("kafka", "auto_offset_reset")
    })

    # subscribe to topic
    consumer.subscribe(topics=[config.get("kafka", "topic")])

    # consume messages
    try:
        while True:
            msg = consumer.poll(timeout=2.0)

            if msg is None:
                logging.debug("No message received")
                continue
            if msg.error():
                logging.error(f"Consumer error: {msg.error()}")
                continue
            else:
                logging.info(f"Received message: {msg.value().decode('utf-8')}")

    except KeyboardInterrupt:
        logging.info("Stopping Kafka consumer")
        consumer.close()


if __name__ == "__main__":

    # start logging
    logging.config.fileConfig("logging.ini")
    logger = logging.getLogger(__name__)

    run_kafka_consumer()
