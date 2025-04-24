variable "credentials" {
  description = "Project GCP credentials account"
  default     = "C:/Users/Bagas/nyc-taxi-etl/credentials/cred_file.json"
}

variable "project" {
  description = "Project ID"
  default     = "indigo-muse-452811-u7"
}

variable "bucket_name_raw" {
  description = "Raw Ingest Bucket"
  default     = "project_raw_ingest"
}

variable "bucket_name_processed" {
  description = "Raw Ingest Bucket"
  default     = "project_temp_data"
}

variable "region" {
  description = "Datalake Region"
  default     = "asia-southeast2"
}

variable "location" {
  description = "Default location"
  default     = "ASIA"
}

variable "GCS_storage_class" {
  description = "Storage Default Class"
  default     = "STANDART"
}

variable "bq_dataset_name" {
  description = "Big Query Dataset Name"
  default     = "project_dataset"
}


