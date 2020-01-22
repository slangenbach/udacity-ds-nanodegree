import os
import logging
import logging.config
from configparser import ConfigParser

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *


def run_spark_job(spark: SparkSession, config: ConfigParser):
    """
    tbd
    """

    # set log level for Spark app
    spark.sparkContext.setLogLevel("WARN")

    # start reading data from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config.get("spark", "bootstrap_servers")) \
        .option("subscribe", config.get("kafka", "topic")) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 200) \
        .option("maxRatePerPartition", 10) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # print schema of incoming data
    #df.printSchema()

    # define schema for incoming data
    schema = StructType([
        StructField("crime_id", StringType(), False),
        StructField("original_crime_type_name", StringType(), True),
        StructField("report_date", TimestampType(), True),
        StructField("call_date", TimestampType(), True),
        StructField("offense_date", TimestampType(), True),
        StructField("call_time", StringType(), True),
        StructField("call_date_time", TimestampType(), True),
        StructField("disposition", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("agency_id", StringType(), True),
        StructField("address_type", StringType(), True),
        StructField("common_location", StringType(), True)
    ])

    # extract value of incoming Kafka data, ignore key
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df \
        .select(F.from_json(kafka_df.value, schema).alias("DF")) \
        .select("DF.*")

    # select original_crime_type_name, disposition and call_date_time (required for watermark)
    distinct_table = service_table \
            .select("original_crime_type_name", "disposition", "call_date_time") \
            .withWatermark("call_date_time", "10 minute")

    # count the number of original crime type
    agg_df = distinct_table.groupBy("original_crime_type_name").count().sort("count", ascending=False)

    # write output stream  TODO: Record
    logger.debug("Streaming count of crime types")
    query = agg_df \
        .writeStream \
        .trigger(processingTime="10 seconds") \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()

    # load radio code data
    radio_code_df = spark.read.json(config.get("spark", "input_file"))

    # rename disposition_code column to disposition in order to join
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # join radio codes to aggregate data on disposition column
    logger.debug("Joining aggregated data and radio codes")
    join_query = agg_df.join(radio_code_df, "disposition", "left")

    join_query.awaitTermination()


if __name__ == "__main__":

    # load config
    config = ConfigParser()
    config.read("app.ini")

    # start logging
    logging.config.fileConfig("logging.ini")
    logger = logging.getLogger(__name__)

    # define environment variables
    os.environ["JAVA_HOME"] = config.get("spark", "java_home")

    # create spark session
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("crime_statistics_stream") \
        .getOrCreate()

    logger.info("Starting Spark Job")
    run_spark_job(spark, config)

    logger.info("Closing Spark Session")
    spark.stop()
