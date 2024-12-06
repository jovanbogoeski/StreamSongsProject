# StreamsSongs

A data pipeline with Kafka, Spark Streaming, dbt, Docker, Airflow, Terraform, GCP

## Description

### Objective

The project will stream events generated from a fake music streaming service (like Spotify) and create a data pipeline that consumes the real-time data. The data coming in would be similar to an event of a user listening to a song, navigating on the website, authenticating. The data would be processed in real-time and stored to the data lake periodically (every two minutes). The hourly batch job will then consume this data, apply transformations, and create the desired tables for our dashboard to generate analytics. We will try to analyze metrics like popular songs, active users, user demographics etc.

### Dataset

[Eventsim](https://github.com/Interana/eventsim) is a program that generates event data to replicate page requests for a fake music web site. The results look like real use data, but are totally fake. The docker image is borrowed from [viirya's fork](https://github.com/viirya/eventsim) of it, as the original project has gone without maintenance for a few years now.

Eventsim uses song data from [Million Songs Dataset](http://millionsongdataset.com) to generate events.

### Tools & Technologies

- Cloud - [**Google Cloud Platform**]
- Infrastructure as Code software - [**Terraform**](https://www.terraform.io)
- Containerization - [**Docker**], [**Docker Compose**]
- Stream Processing - [**Kafka**], [**Spark Streaming**]
- Orchestration - [**Airflow**]
- Transformation - [**dbt**]
- Data Lake - [**Google Cloud Storage**]
- Data Warehouse - [**BigQuery**]
- Data Visualization - [**Power BI**]
- Language - [**Python**]

