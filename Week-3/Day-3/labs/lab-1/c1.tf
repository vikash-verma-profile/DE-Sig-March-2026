provider "google" {
  project = "your-project-id"
  region  = "asia-south1"
}

resource "google_storage_bucket" "my_bucket" {
  name          = "my-unique-bucket-name-12345"
  location      = "ASIA-SOUTH1"
  force_destroy = true

  storage_class = "STANDARD"

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    env = "dev"
  }
}