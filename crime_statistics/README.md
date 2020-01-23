# SF Crime Statistics with Spark Structured Streaming

## About
The scripts within this repository simulate the production, consumption and analysis of crime statistics data using 
Apache Kafka and Apache Spark. Once required infrastructure has been set up (`docker-compose.yaml`), raw crime data
is read from disk. Individual incidents are then streamed (`kafka_producer.py`) to Apache Kafka. Once stored in 
Kafka, incidents are analysed with Spark Structured Streaming (`spark_streaming.py`). For debugging purposes they can 
also be retrieved using a dedicated consumer (`kafka_consumer.py`).

## Demo
In order to see producer, consumer and Spark Streaming in action, check out the corresponding recordings on 
https://asciinema.org.

### Producer
[![asciicast](https://asciinema.org/a/0zHZl7C7fvWzlkghCHOWcwGt5.svg)](https://asciinema.org/a/0zHZl7C7fvWzlkghCHOWcwGt5)

### Consumer
[![asciicast](https://asciinema.org/a/iRIs7eSSkHHwhPw1BUrzQM1Y3.svg)](https://asciinema.org/a/iRIs7eSSkHHwhPw1BUrzQM1Y3)

### Spark Streaming aggregation
[![asciicast](https://asciinema.org/a/uwJHvosvWbJsEsoR1Mr1LqE4y.svg)](https://asciinema.org/a/uwJHvosvWbJsEsoR1Mr1LqE4y)

### Spark Streaming join
[![asciicast](https://asciinema.org/a/bYL5DFydws0KbkFNe4LXc6QLV.svg)](https://asciinema.org/a/bYL5DFydws0KbkFNe4LXc6QLV)

## Prerequisites
* Access to raw SF crime statistics data in JSON format
* Docker Desktop 2.1+
* Anaconda Python 3.7+
* Java 8 (in order to run Spark jobs locally)
* Unix-like environment (Linux, macOS, WSL on Windows)

## Usage
1. Clone this repository and navigate into _crime_statistics_ directory
2. Set up required infrastructure with docker via `docker-compose up`
3. If runningn Spark locally: Double check if Java 8 is installed on your machine with `java -version`, and if 
`JAVA_HOME` is set correctly via `echo $JAVA_HOME`.
4. Create a dedicated Python environment using conda via `conda env create -f conda_env.yml` 
and activate it via `conda activate udacity-dsnd`
5. Optionally edit `app.cfg` in order to change configuration settings for Kafka and Spark
6. Open a terminal and start producing data to Kafka via `python kafka_producer.py` 
7. Optionally check if data is correctly produced by opening **another** terminal and running `python kafka_consumer.py`
8. Open **another** terminal and analyze data via Spark Structured Streaming using 
`spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 spark_streaming.py` 
9. Monitor the Spark job via Sparks web UI at http://localhost:4040 if running locally, or at http://localhost:8080 if 
running on a stand alone cluster.
 
# Limitations
* Docker-Compose uses a third-party Apache Spark image
([gettyimages/spark](https://hub.docker.com/r/gettyimages/spark/)) since the Spark maintainers do not provide an 
official image. Unfortunately the latest release of that image does not use the latest version of Spark, but version 
2.4.1.

# Resources
* [Spark Structured Streaming Programming Guide](https://spark.apache.org/docs/2.4.1/structured-streaming-programming-guide.html)
* [Spark Structured Streaming Kafka Integration](https://spark.apache.org/docs/2.4.1/structured-streaming-kafka-integration.html)
* [Spark: Submitting applications](https://spark.apache.org/docs/latest/submitting-applications.html)
* [Reading multiline JSON files with Spark](https://docs.databricks.com/data/data-sources/read-json.html#multi-line-mode)
* [Udacity knowledge portal](https://knowledge.udacity.com)
