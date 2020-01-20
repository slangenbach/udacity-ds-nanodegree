import logging
import logging.config
from configparser import ConfigParser

import producer_server


def run_kafka_server(config: ConfigParser) -> producer_server:
    """
    tbd
    """
    producer = producer_server.ProducerServer(
        input_file=config.get("kafka", "input_file"),
        topic=config.get("kafka", "topic"),
        conf = {
            "bootstrap.servers": config.get("bootstrap_servers"),
            "client.id": config.get("client_id")
        }
    )

    return producer


if __name__ == "__main__":

    # load config
    config = ConfigParser()
    config.read("app.ini")

    # start logging
    logging.config.fileConfig("logging.ini")
    logger = logging.getLogger(__name__)
    logger.info("Starting")

    # start kafka server and generate data
    logger.info("Starting Kafka Producer")
    producer = run_kafka_server(config)

    # check if topic exists
    producer.create_topic()

    logger.info("Start generating data...")
    producer.generate_data()
