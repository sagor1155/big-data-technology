#!/bin/bash
echo "............Starting Spark SQL Client..........."
spark-submit --class "cs523.spark.sqlclient.SparkSQLClient" --master local[*]  '/home/cloudera/project/spark-sql-client/target/spark-sql-client-jar-with-dependencies.jar'
