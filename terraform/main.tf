terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.8.0"
    }
  }
}

provider "google" {
  project     = var.project
  region      = var.region
  credentials = file(var.credentials)
}

resource "google_storage_bucket" "bucket_processed" {
  name          = var.bucket_name_processed
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 60
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_storage_bucket" "bucket_raw" {
  name          = var.bucket_name_raw
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 60
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id  = var.bq_dataset_name
  description = "Project Dataset for NYC ETL"
  location    = "US"
}

resource "google_bigquery_table" "table_staging" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "staging_table"
}

resource "google_bigquery_table" "table_production" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "production_table"
}
