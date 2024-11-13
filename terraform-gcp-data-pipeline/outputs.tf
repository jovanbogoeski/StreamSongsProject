output "kafka_ip" {
  description = "Public IP address of the Kafka instance"
  value       = google_compute_instance.kafka.network_interface[0].access_config[0].nat_ip
}

output "spark_ip" {
  description = "Public IP address of the Spark instance"
  value       = google_compute_instance.spark.network_interface[0].access_config[0].nat_ip
}

output "airflow_ip" {
  description = "Public IP address of the Airflow instance"
  value       = google_compute_instance.airflow.network_interface[0].access_config[0].nat_ip
}

output "storage_bucket_name" {
  description = "Name of the Google Cloud Storage bucket"
  value       = google_storage_bucket.data_bucket.name
}

output "bigquery_dataset_id" {
  description = "ID of the BigQuery dataset for processed data"
  value       = google_bigquery_dataset.data_pipeline_dataset.dataset_id
}

output "vpc_name" {
  description = "Name of the VPC network"
  value       = google_compute_network.vpc_network.name
}

output "subnet_name" {
  description = "Name of the subnet within the VPC"
  value       = google_compute_subnetwork.subnet.name
}
