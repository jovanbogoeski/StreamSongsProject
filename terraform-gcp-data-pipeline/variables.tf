# Project and Region Configurations
variable "project_id" {
  description = "The Google Cloud project ID"
  type        = string
  default     = "your-project-id"  # Replace with your actual GCP project ID
}

variable "region" {
  description = "The region where resources will be deployed"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "The GCP zone where resources will be deployed"
  type        = string
  default     = "us-central1-a"
}

# Instance Configurations
variable "kafka_machine_type" {
  description = "The machine type for the Kafka instance"
  type        = string
  default     = "n1-standard-2"
}

variable "spark_machine_type" {
  description = "The machine type for the Spark instance"
  type        = string
  default     = "n1-standard-4"
}

variable "airflow_machine_type" {
  description = "The machine type for the Airflow instance"
  type        = string
  default     = "n1-standard-2"
}

# Networking Configurations
variable "vpc_name" {
  description = "The name of the VPC network"
  type        = string
  default     = "data-pipeline-vpc"
}

variable "subnet_name" {
  description = "The name of the subnet within the VPC"
  type        = string
  default     = "data-pipeline-subnet"
}

variable "subnet_cidr" {
  description = "The IP CIDR range for the subnet"
  type        = string
  default     = "10.0.0.0/16"
}

# Firewall Configurations
variable "allowed_ports" {
  description = "List of allowed ports for firewall rules"
  type        = list(number)
  default     = [22, 7077, 9092]  # Example ports for SSH, Spark, and Kafka
}

# Google Cloud Storage Configurations
variable "bucket_name_prefix" {
  description = "The prefix for the Google Cloud Storage bucket name"
  type        = string
  default     = "data-pipeline-bucket"
}

variable "bucket_location" {
  description = "Location for the Google Cloud Storage bucket"
  type        = string
  default     = "US"
}

variable "bucket_storage_class" {
  description = "Storage class for the Google Cloud Storage bucket"
  type        = string
  default     = "STANDARD"
}

# BigQuery Dataset Configurations
variable "bigquery_dataset_id" {
  description = "The ID of the BigQuery dataset for processed data"
  type        = string
  default     = "data_pipeline"
}

variable "bigquery_location" {
  description = "Location of the BigQuery dataset"
  type        = string
  default     = "US"
}
