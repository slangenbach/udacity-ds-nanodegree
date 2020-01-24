#!/bin/zsh
echo "Setting JAVA_HOME"
export JAVA_HOME=/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home/
echo "Submitting Spark job"
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 spark_streaming.py