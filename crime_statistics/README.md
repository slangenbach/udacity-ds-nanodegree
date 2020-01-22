# SF Crime Statistics with Spark Streaming

## About
The scripts within this repository simulate the production, consumption and analysis of crime statistics data using 
Apache Kafka and Apache Spark. Once required infrastructure has been set up (c.f. `docker-compose.yaml`), raw crime data
is read from disk. Individual incidents are then streamed (c.f. `kafka_producer.py`) to Apache Kafka. Once stored in 
Kafka, incidents are analysed with Spark Structured Streaming (c.f. `spark_stream.py`). For debugging purposes they can 
also be retrieved using a dedicated consumer (c.f. `kafka_consumer.py`).

## Demo
In order to see producer, consumer and Spark Streaming in action, check out the corresponding recordings on 
https://asciinema.org.

### Producer
[![asciicast](https://asciinema.org/a/0zHZl7C7fvWzlkghCHOWcwGt5.svg)](https://asciinema.org/a/0zHZl7C7fvWzlkghCHOWcwGt5)

### Consumer
[![asciicast](https://asciinema.org/a/iRIs7eSSkHHwhPw1BUrzQM1Y3.svg)](https://asciinema.org/a/iRIs7eSSkHHwhPw1BUrzQM1Y3)

### Spark Streaming
[![asciicast](https://asciinema.org/a/uwJHvosvWbJsEsoR1Mr1LqE4y.svg)](https://asciinema.org/a/uwJHvosvWbJsEsoR1Mr1LqE4y)

## Implementation details
How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

What were the 2-3 most efficient SparkSession property key/value pairs? 

Through testing multiple variations on values, how can you tell these were the most optimal?

## Prerequisites
* Access to raw SF crime statistics data in JSON format
* Docker Desktop 2.1+
* Anaconda Python 3.7+
* Java 8 (in order to submit Spark jbs)
* Unix-like environment (Linux, macOS, WSL on Windows)

## Usage
1. Clone this repository and navigate into _crime_statistics_ directory
2. Set up required infrastructure with docker via `docker-compose up`
3. Double check if Java 8 is installed on your machine (`java -version`) and if `JAVA_HOME` is set correctly 
(`echo $JAVA_HOME`).
4. Create a dedicated Python environment using conda via `conda env create -f conda_env.yml` 
and activate it via `conda actiate udacity-dsnd`
5. Optionally edit `app.cfg` in order to change configuration settings for Kafka and Spark
6. Open a terminal and start producing data to Kafka via `python kafka_producer.py` 
7. Optionally check if data is correctly produced by opening *another* terminal and running `python kafka_consumer.py`
8. Open *another* terminal and analyze data via Spark Structured Streaming using 
`spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 spark_streaming.py` 
9. Monitor the Spark job via Sparks web UI at http://localhost:8080. If you need information on completed/terminated 
jobs use Sparks history server at http://localhost:18080
 
# Limitations
* Docker-Compose uses a third-party Apache Spark image
(c.f. [gettyimages/spark](https://hub.docker.com/r/gettyimages/spark/)) since the Spark maintainers do not provide an 
official image. Unfortunately the latest release of that image does not use the latest version of Spark, but version 
2.4.1

# Resources
* [Spark Structured Streaming Programming Guide](https://spark.apache.org/docs/2.4.1/structured-streaming-programming-guide.html)
* [Spark Structured Streaming Kafka Integration](https://spark.apache.org/docs/2.4.1/structured-streaming-kafka-integration.html)
* [Udacity knowledge portal](https://knowledge.udacity.com)
