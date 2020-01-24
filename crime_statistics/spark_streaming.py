import logging
import logging.config
from configparser import ConfigParser

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json


def run_spark_job(spark: SparkSession, config: ConfigParser):
    """
    Run Spark Structured Streaming job reading data from Kafka
    """

    # set log level for Spark app
    spark.sparkContext.setLogLevel("WARN")

    # define schema for incoming data
    kafka_schema = StructType([
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

    radio_schema = StructType([
        StructField("disposition_code", StringType(), True),
        StructField("description", StringType(), True)
    ])

    # start reading data from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config.get("spark", "bootstrap_servers")) \
        .option("subscribe", config.get("kafka", "topic")) \
        .option("startingOffsets", config.get("spark", "starting_offsets")) \
        .option("maxOffsetsPerTrigger", config.get("spark", "max_offsets_per_trigger")) \
        .option("maxRatePerPartition", config.get("spark", "max_rate_per_partition")) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # print schema of incoming data
    logging.debug("Printing schema of incoming data")
    df.printSchema()

    # extract value of incoming Kafka data, ignore key
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df \
        .select(from_json(kafka_df.value, kafka_schema).alias("DF")) \
        .select("DF.*")

    # select original_crime_type_name, disposition and call_date_time (required for watermark)
    distinct_table = service_table \
        .select("original_crime_type_name", "disposition", "call_date_time") \
        .withWatermark("call_date_time", "10 minutes")

    # load radio code data
    logger.debug("Reading static data from disk")
    radio_code_df = spark \
        .read \
        .option("multiline", "true") \
        .json(path=config.get("spark", "input_file"), schema=radio_schema)

    # rename disposition_code column to disposition in order to join
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # join radio codes to distinct_table on disposition column
    logger.debug("Joining aggregated data and radio codes")
    join_df = distinct_table \
        .join(radio_code_df, "disposition", "left") \
        .select("call_date_time", "original_crime_type_name", "description")

    # count the number of original crime type
    agg_df = distinct_table.groupBy("original_crime_type_name").count().sort("count", ascending=False)

    # write output stream
    logger.info("Streaming count of crime types")
    agg_query = agg_df \
        .writeStream \
        .trigger(processingTime="10 seconds") \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    agg_query.awaitTermination()

    logger.info("Streaming crime types and descriptions")
    join_query = join_df \
        .writeStream \
        .trigger(processingTime="10 seconds") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    join_query.awaitTermination()


if __name__ == "__main__":
    # load config
    config = ConfigParser()
    config.read("app.cfg")

    # start logging
    logging.config.fileConfig("logging.ini")
    logger = logging.getLogger(__name__)

    # create spark session
    spark = SparkSession \
        .builder \
        .master(config.get("spark", "master")) \
        .appName("crime_statistics_stream") \
        .getOrCreate()

    logger.info("Starting Spark Job")
    run_spark_job(spark, config)

    logger.info("Closing Spark Session")
    spark.stop()
