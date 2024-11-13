#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Update and install required packages
sudo apt-get update -y
sudo apt-get install -y openjdk-11-jdk wget

# Define variables
KAFKA_VERSION="2.8.0"
SCALA_VERSION="2.13"
KAFKA_DIR="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
KAFKA_TGZ="${KAFKA_DIR}.tgz"
KAFKA_URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_TGZ}"
LOG_DIR="/tmp"

# Download and extract Kafka
wget $KAFKA_URL -P /tmp
tar -xzf /tmp/$KAFKA_TGZ -C /tmp
cd /tmp/$KAFKA_DIR

# Configure Kafka properties
echo "broker.id=0" >> config/server.properties
echo "listeners=PLAINTEXT://:9092" >> config/server.properties
echo "log.dirs=${LOG_DIR}/kafka-logs" >> config/server.properties

# Start Zookeeper (required for Kafka)
nohup bin/zookeeper-server-start.sh config/zookeeper.properties > ${LOG_DIR}/zookeeper.log 2>&1 &

# Give Zookeeper a few seconds to start
sleep 5

# Start Kafka server
nohup bin/kafka-server-start.sh config/server.properties > ${LOG_DIR}/kafka.log 2>&1 &
