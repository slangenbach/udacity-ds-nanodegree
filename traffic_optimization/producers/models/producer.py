"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
            self,
            topic_name,
            key_schema,
            value_schema=None,
            num_partitions=1,
            num_replicas=1
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self.broker_properties = {
            'bootstrap.servers': 'PLAINTEXT://0.0.0.0:9092,PLAINTEXT://0.0.0.0:9093,PLAINTEXT://0.0.0.0:9094',
            'schema.registry.url': 'http://0.0.0.0:8081'
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            config=self.broker_properties,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema)

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""

        logger.debug("producer topic creation function working")
        client = AdminClient({'bootstrap.servers': self.broker_properties.get('bootstrap.servers')})

        if self.topic_name not in client.list_topics().topics:
            futures = client.create_topics([NewTopic(topic=self.topic_name,
                                                     num_partitions=self.num_partitions,
                                                     replication_factor=self.num_replicas)])

            for topic, future in futures.items():
                try:
                    future.result()
                    logger.info(f"Topic {topic} created")
                except Exception as e:
                    logger.fatal(f"Failed to create topic {topic}: {e}")
        else:
            logger.info(f"Topic {self.topic_name} already exists")

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        logger.debug("producer close function working")

        if self.producer is not None:
            logger.debug("Flushing producer")
            self.producer.flush()

    @staticmethod
    def time_millis():
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
