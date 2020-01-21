import logging
import logging.config
from configparser import ConfigParser

# make sure logging config is picked up by modules
logging.config.fileConfig("logging.ini")

import producer_server


def run_kafka_producer():
    """
    tbd
    """

    # load config
    config = ConfigParser()
    config.read("app.ini")

    # start kafka producer
    logger.info("Starting Kafka Producer")
    producer = producer_server.ProducerServer(config)

    # check if topic exists
    logger.info("Creating topic...")
    producer.create_topic()

    # generate data
    logger.info("Starting to generate data...")

    try:
        producer.generate_data()
    except KeyboardInterrupt:
        logging.info("Stopping Kafka Producer")
        producer.close()


if __name__ == "__main__":

    # start logging
    logger = logging.getLogger(__name__)

    run_kafka_producer()
