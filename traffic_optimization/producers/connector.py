"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging
from configparser import ConfigParser

import requests

logger = logging.getLogger(__name__)


def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.info("creating or updating kafka connect connector...")

    config = ConfigParser()
    config.read("app_config.ini")

    connect_url = f"{config.get('hosts', 'kafka_connect')}/connectors/{config.get('kafka-connect', 'connector_name')}"
    resp = requests.get(connect_url)
    if resp.status_code == 200:
        logging.debug("connector already created, skipping recreation")
        return

    logger.debug("Kafka connector code working")
    resp = requests.post(
        f"{config.get('hosts', 'kafka_connect')}/connectors",
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "name": config.get('kafka-connect', 'connector_name'),
            "config": {
                "connector.class": config.get('kafka-connect', 'connector_class'),
                "key.converter": config.get('kafka-connect', 'key_converter'),
                "key.converter.schemas.enable": config.get('kafka-connect', 'key_converter_schemas_enable'),
                "value.converter": config.get('kafka-connect', 'value_converter'),
                "value.converter.schemas.enable": config.get('kafka-connect', 'value_converter_schemas_enable'),
                "batch.max.rows": config.get('kafka-connect', 'batch_max_rows'),
                "connection.url": config.get('postgres', 'url'),
                "connection.user": config.get('kafka-connect', 'connection_user'),
                "connection.password": config.get('kafka-connect', 'connection_password'),
                "table.whitelist": config.get('kafka-connect', 'table_whitelist'),
                "mode": config.get('kafka-connect', 'mode'),
                "incrementing.column.name": config.get('kafka-connect', 'incrementing_column_name'),
                "topic.prefix": config.get('kafka-connect', 'topic_prefix'),
                "poll.interval.ms": config.get('kafka-connect', 'poll_interval')  # 24h
            }
        }),
    )

    # Ensure a healthy response was given
    resp.raise_for_status()
    logging.debug("connector created successfully")


if __name__ == "__main__":
    configure_connector()
