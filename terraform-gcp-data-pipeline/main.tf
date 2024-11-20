provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

# VPC Network
resource "google_compute_network" "vpc_network" {
  name = "data-pipeline-vpc"
}

# Subnetwork
variable "subnet_cidr" {
  description = "CIDR range for the subnet"
  default     = "10.0.0.0/16"
}

resource "google_compute_subnetwork" "subnet" {
  name          = "data-pipeline-subnet"
  ip_cidr_range = var.subnet_cidr
  region        = var.region
  network       = google_compute_network.vpc_network.id
}

# Firewall Rules
resource "google_compute_firewall" "allow_internal" {
  name    = "allow-internal"
  network = google_compute_network.vpc_network.name

  allow {
    protocol = "tcp"
    ports    = ["22", "7077", "8080", "9092"]  # SSH, Spark Master, Airflow UI, Kafka
  }

  source_ranges = ["10.0.0.0/16"]
}

resource "google_compute_firewall" "allow_ssh_external" {
  name    = "allow-ssh-external"
  network = google_compute_network.vpc_network.name

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["<your-external-ip>/32"]  # Replace with your IP
}

# Service Accounts
resource "google_service_account" "kafka_sa" {
  account_id   = "kafka-service-account"
  display_name = "Kafka Service Account"
}

resource "google_service_account" "spark_sa" {
  account_id   = "spark-service-account"
  display_name = "Spark Service Account"
}

resource "google_project_iam_member" "kafka_storage_access" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.kafka_sa.email}"
}

resource "google_project_iam_member" "spark_storage_access" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.spark_sa.email}"
}

# Compute Instances
resource "google_compute_instance" "kafka" {
  name         = "kafka-instance"
  machine_type = var.kafka_machine_type
  zone         = var.zone
  service_account {
    email  = google_service_account.kafka_sa.email
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }
  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-10"
    }
  }
  network_interface {
    network    = google_compute_network.vpc_network.id
    subnetwork = google_compute_subnetwork.subnet.name
    access_config {}
  }
  metadata_startup_script = file("kafka-startup.sh")
}

resource "google_compute_instance" "spark" {
  name         = "spark-instance"
  machine_type = var.spark_machine_type
  zone         = var.zone
  service_account {
    email  = google_service_account.spark_sa.email
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }
  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-10"
    }
  }
  network_interface {
    network    = google_compute_network.vpc_network.id
    subnetwork = google_compute_subnetwork.subnet.name
    access_config {}
  }
  metadata_startup_script = file("spark-startup.sh")
}

# Google Cloud Storage Bucket
resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "google_storage_bucket" "data_bucket" {
  name          = "${var.bucket_name_prefix}-${random_id.bucket_suffix.hex}"
  location      = var.bucket_location
  storage_class = var.bucket_storage_class

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  # Deletes objects older than 30 days
    }
  }
}

# BigQuery Dataset
resource "google_bigquery_dataset" "data_pipeline_dataset" {
  dataset_id = "streaming_data_${var.environment}"
  location   = var.bigquery_location
}
