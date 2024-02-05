#!/bin/bash
echo "............Starting Spark Streaming Consumer Application..........."
spark-submit --class "cs523.spark.streaming.SparkKafkaConsumer" --master local[*]  '/home/cloudera/workspace/spark-kafka-streaming/target/spark-consumer-jar-with-dependencies.jar'
