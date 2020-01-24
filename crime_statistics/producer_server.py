import json
import logging
import time

from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

logger = logging.getLogger(__name__)


class ProducerServer:
    """
    Basic Kafka consumer class
    """

    def __init__(self, conf):
        self.conf = conf
        self.topic = self.conf.get("kafka", "topic")
        self.input_file = self.conf.get("kafka", "input_file")
        self.bootstrap_servers = self.conf.get("kafka", "bootstrap_servers")
        self.num_partitions = self.conf.getint("kafka", "num_partitions")
        self.replication_factor = self.conf.getint("kafka", "replication_factor")
        self.progress_interval = self.conf.getint("kafka", "progress_interval")
        self.admin_client = AdminClient({"bootstrap.servers": self.bootstrap_servers})
        self.producer = Producer({"bootstrap.servers": self.bootstrap_servers})

    def create_topic(self):
        """
        Check if Kafka topic already exists. If not, create it, else continue
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
        Read input JSON file from disk and produce individual serialized rows to Kafka
        """
        with open(self.input_file, "r") as f:

            # read JSON data from input file
            data = json.loads(f.read())
            logger.debug(f"Read {len(data)} lines from input file: {self.input_file}")

            for idx, row in enumerate(data):

                # trigger delivery report callbacks from previous produce calls
                self.producer.poll(timeout=2)

                # serialize Python dict to string
                msg = self.serialize_json(row)
                logger.debug(f"Serialized JSON data: {msg}")

                # send data to Kafka
                self.producer.produce(topic=self.topic, value=msg, callback=self.delivery_callback)

                # log progress
                if idx % self.progress_interval == 0:
                    logger.debug(f"Processed {idx} rows of data")

                # wait 2 second before reading next line
                time.sleep(2)

            # make sure all messages are delivered before closing producer
            logger.debug("Flushing producer")
            self.producer.flush()

    @staticmethod
    def serialize_json(json_data):
        """
        Serialize Python dict to JSON-formatted, UTF-8 encoded string
        """
        return json.dumps(json_data).encode("utf-8")

    @staticmethod
    def delivery_callback(err, msg):
        """
        Callback triggered by produce function
        """
        if err is not None:
            logger.error(f"Failed to deliver message: {err}")
        else:
            logger.info(f"Successfully produced message to topic {msg.topic()}")

    def close(self):
        """
        Convenience method to flush producer
        """
        logger.debug("Flushing producer")
        self.producer.flush()
