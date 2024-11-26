variable "project" {
  description = "Your GCP Project ID. Must be set explicitly."
  type        = string
}

variable "region" {
  description = "The region for your resources (e.g., us-central1)."
  default     = "us-central1"
  type        = string
}

variable "zone" {
  description = "The zone for your resources (e.g., us-central1-a)."
  default     = "us-central1-a"
  type        = string
  validation {
    condition     = can(regex("^[a-z]+-[a-z]+[0-9]-[a-z]$", var.zone))
    error_message = "Zone must be in the format <region>-<zone> (e.g., us-central1-a)."
  }
}

variable "storage_class" {
  description = "Storage class type for your bucket (e.g., STANDARD, NEARLINE, COLDLINE)."
  default     = "STANDARD"
  type        = string
}

variable "vm_image" {
  description = "Image for your VM instance (e.g., ubuntu-os-cloud/ubuntu-2004-lts)."
  default     = "ubuntu-os-cloud/ubuntu-2004-lts"
  type        = string
}

variable "network" {
  description = "VPC network for your instances and clusters."
  default     = "default"
  type        = string
}

variable "stg_bq_dataset" {
  description = "BigQuery staging dataset ID for temporary or intermediate data."
  default     = "streamify_stg"
  type        = string
}

variable "prod_bq_dataset" {
  description = "BigQuery production dataset ID for final processed data."
  default     = "streamify_prod"
  type        = string
}

variable "bucket" {
  description = "The name of your Cloud Storage bucket. Must be unique across GCP."
  type        = string
  validation {
    condition     = length(var.bucket) > 3 && length(var.bucket) <= 63
    error_message = "Bucket name must be between 3 and 63 characters long."
  }
}
