import json
import logging
import time

from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic


logger = logging.getLogger(__name__)


class ProducerServer(Producer):
    """
    tbd
    """

    def __init__(self, input_file, topic, conf, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic
        self.conf = conf

    def create_topic(self):
        """
        tbd
        """
        client = AdminClient(conf={"bootstrap.servers": self.conf.get("bootstrap.servers")})
        if self.topic not in client.list_topics().topics:
            futures = client.create_topics([NewTopic(topic=self.topic)])

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
        with open(self.input_file) as f:
            for line in f:

                logger.debug("Encoding line to JSON")
                msg = json.dumps(line)

                logger.debug("Sending encoded data to Kafka")
                self.produce(topic=self.topic, value=msg.encode("utf-8"), callback=self.delivery_callback)


                # wait 1 second before reading next line
                time.sleep(1)

            # make sure all messages are delivered before closing producer
            logging.debug("Flushing producer")
            self.flush()

    def delivery_callback(self, msg, err):
        """
        tbd
        """
        if err is not None:
            logging.error(f"Failed to deliver message {msg}: {err}")
        else:
            logging.info(f"Successfully produced message: {msg}")

        