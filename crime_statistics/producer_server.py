import json
import logging
import time

from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic


logger = logging.getLogger(__name__)


class ProducerServer():
    """
    tbd
    """

    def __init__(self, conf):
        self.conf = conf
        self.topic = self.conf.get("kafka", "topic")
        self.input_file = self.conf.get("kafka", "input_file")
        self.bootstrap_servers = self.conf.get("kafka", "bootstrap_servers")
        self.num_partitions = self.conf.getint("kafka", "num_partitions")
        self.replication_factor = self.conf.getint("kafka", "replication_factor")
        self.admin_client = AdminClient({"bootstrap.servers": self.bootstrap_servers})
        self.producer = Producer({"bootstrap.servers": self.bootstrap_servers})

    def create_topic(self):
        """
        tbd
        """
        if self.topic not in self.admin_client.list_topics().topics:
            futures = self.admin_client.create_topics([NewTopic(topic=self.topic,
                                                     num_partitions=self.num_partitions,
                                                     replication_factor=self.replication_factor)])

            for _topic, future in futures.items():
                try:
                    future.result()
                    logger.info(f"Created topic: {_topic}")
                except KafkaError as err:
                    logger.critical(f"Failed to create topic {_topic}: {err}")
        else:
            logger.info(f"Topic {self.topic} already exists")

    def generate_data(self):
        """
        tbd
        """
        with open(self.input_file, "r") as f:
            lines = json.loads(f.read())
            for ix, line in enumerate(lines):

                # trigger delivery report callbacks from previous produce calls
                self.producer.poll(timeout=1)

                logger.debug("Encoding line to JSON")
                msg = json.dumps(line)

                logger.debug(f"Sending encoded data to Kafka: {msg}")
                self.producer.produce(topic=self.topic, value=msg.encode("utf-8"), callback=self.delivery_callback)

                # wait 1 second before reading next line
                time.sleep(1)

            # make sure all messages are delivered before closing producer
            logger.debug("Flushing producer")
            self.producer.flush()

    def delivery_callback(self, msg, err):
        """
        tbd
        """
        if err is not None:
            logger.error(f"Failed to deliver message: {err}")
        else:
            logger.info(f"Successfully produced message: {msg}")

    def close(self):
        logger.debug("Flushing producer")
        self.producer.flush()