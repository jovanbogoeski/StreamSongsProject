#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Update and install Java and Scala
sudo apt-get update -y
sudo apt-get install -y openjdk-11-jdk scala wget

# Define variables
SPARK_VERSION="3.1.2"
SPARK_HADOOP_VERSION="hadoop3.2"
SPARK_DIR="spark-${SPARK_VERSION}-bin-${SPARK_HADOOP_VERSION}"
SPARK_TGZ="${SPARK_DIR}.tgz"
SPARK_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}"
LOG_DIR="/tmp"

# Download and extract Spark
wget $SPARK_URL -P $LOG_DIR
tar -xzf ${LOG_DIR}/${SPARK_TGZ} -C $LOG_DIR
cd ${LOG_DIR}/${SPARK_DIR}

# Set SPARK_HOME environment variable
export SPARK_HOME=${LOG_DIR}/${SPARK_DIR}
export PATH=$SPARK_HOME/bin:$PATH

# Start Spark master
nohup $SPARK_HOME/sbin/start-master.sh > ${LOG_DIR}/spark-master.log 2>&1 &
