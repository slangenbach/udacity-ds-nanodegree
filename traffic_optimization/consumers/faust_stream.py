"""Defines trends calculations for stations"""
import logging
from dataclasses import dataclass

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
@dataclass
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
@dataclass
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://0.0.0.0:9092", store="memory://")
topic = app.topic("org.chicago.cta.stations", value_type=Station)
out_topic = app.topic("org.chicago.cta.stations.table.v1", value_type=TransformedStation, partitions=1)

table = app.Table(
    "stations",
    default=int,
    partitions=1,
    changelog_topic=out_topic)


@app.agent(topic)
async def transform_station(stream):
    async for event in stream:
        """Transform station data"""
        table["station_id"] = event.station_id
        table["station_name"] = event.station_name
        table["order"] = event.order

        if event.red:
            table["line"] = "red"
        elif event.blue:
            table["line"] = "blue"
        elif event.green:
            table["line"] = "green"


if __name__ == "__main__":
    app.main()
