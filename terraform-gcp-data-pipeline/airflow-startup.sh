
#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Update and install Python3 and pip
sudo apt-get update -y
sudo apt-get install -y python3-pip

# Define environment variables
export AIRFLOW_HOME="/home/airflow"  # Optional: Define a custom Airflow home directory
export PATH=$PATH:~/.local/bin        # Ensure Airflow binaries are in the PATH

# Install Airflow
pip3 install apache-airflow

# Initialize Airflow database
airflow db init

# Create Airflow logs directory if not using the default
mkdir -p $AIRFLOW_HOME/logs

# Start Airflow webserver and scheduler with a slight delay
nohup airflow webserver -p 8080 > $AIRFLOW_HOME/logs/airflow-webserver.log 2>&1 &
sleep 5  # Wait to allow the webserver to start
nohup airflow scheduler > $AIRFLOW_HOME/logs/airflow-scheduler.log 2>&1 &
