# Provider Configuration
provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

# Networking
resource "google_compute_network" "vpc_network" {
  name = var.vpc_name

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_compute_subnetwork" "subnet" {
  name          = var.subnet_name
  ip_cidr_range = var.subnet_cidr
  region        = var.region
  network       = google_compute_network.vpc_network.id

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_compute_firewall" "firewall" {
  name    = "allow-internal"
  network = google_compute_network.vpc_network.name

  allow {
    protocol = "tcp"
    ports    = var.allowed_ports
  }

  source_ranges = ["0.0.0.0/0"]
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

resource "google_service_account" "airflow_sa" {
  account_id   = "airflow-service-account"
  display_name = "Airflow Service Account"
}

# IAM Roles for Service Accounts
resource "google_project_iam_member" "kafka_storage_access" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.kafka_sa.email}"
}

resource "google_project_iam_member" "spark_bigquery_access" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.spark_sa.email}"
}

resource "google_project_iam_member" "airflow_storage_access" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${google_service_account.airflow_sa.email}"
}

# Kafka Instance
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

  lifecycle {
    prevent_destroy = true
  }

  metadata_startup_script = file("kafka-startup.sh")
}

# Spark Instance
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

  lifecycle {
    prevent_destroy = true
  }

  metadata_startup_script = file("spark-startup.sh")
}

# Airflow Instance
resource "google_compute_instance" "airflow" {
  name         = "airflow-instance"
  machine_type = var.airflow_machine_type
  zone         = var.zone
  service_account {
    email  = google_service_account.airflow_sa.email
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

  lifecycle {
    prevent_destroy = true
  }

  metadata_startup_script = file("airflow-startup.sh")
}

# Google Cloud Storage Bucket for Data
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
      age = 30  # Objects older than 30 days will be deleted
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}

# BigQuery Dataset
resource "google_bigquery_dataset" "data_pipeline_dataset" {
  dataset_id = var.bigquery_dataset_id
  location   = var.bigquery_location

  lifecycle {
    prevent_destroy = true
  }
}
