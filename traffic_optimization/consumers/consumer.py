"""Defines core consumer functionality"""
import logging

from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer
from tornado import gen

logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
            self,
            topic_name_pattern,
            message_handler,
            is_avro=True,
            offset_earliest=False,
            sleep_secs=1.0,
            consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest
        self.broker_properties = {
            'bootstrap.servers': 'PLAINTEXT://0.0.0.0:9092,PLAINTEXT://0.0.0.0:9093,PLAINTEXT://0.0.0.0:9094',
            'schema.registry.url': 'http://0.0.0.0:8081',
            'group.id': 'some_id',
        }

        if is_avro is True:
            self.consumer = AvroConsumer(config=self.broker_properties)
        else:
            self.consumer = Consumer({'bootstrap.servers': self.broker_properties.get('bootstrap.servers'),
                                      'group.id': self.broker_properties.get('group.id')})

        self.consumer.subscribe([topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""

        logger.debug("Consumer on_assign function complete")
        for partition in partitions:
            partition.offset = OFFSET_BEGINNING

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""

        logger.debug("Consumer _consume function complete")

        while True:
            message = self.consumer.poll(timeout=1.0)

            if message is None:
                return 0
            elif message.error():
                logger.error(message.error())
            else:
                return 1

    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.close()
