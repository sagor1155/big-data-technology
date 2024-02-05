#!/bin/bash
echo "............Starting Kafka Broker..........."
cd /home/cloudera/Downloads/kafka_2.13-3.6.1
./bin/kafka-server-start.sh config/server.properties

