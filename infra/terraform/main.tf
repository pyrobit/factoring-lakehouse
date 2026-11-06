terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70"
    }
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 1.0"
    }
  }
  backend "s3" {
    bucket         = "factoring-terraform-state"
    key            = "lakehouse/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "terraform-locks"
    encrypt        = true
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = { project = "factoring-lakehouse", env = var.env, owner = "data-platform" }
  }
}

provider "snowflake" {
  organization_name = var.snowflake_org
  account_name      = var.snowflake_account
  user              = var.snowflake_user
  authenticator     = "SNOWFLAKE_JWT"
  private_key       = file(var.snowflake_private_key_path)
  role              = "ACCOUNTADMIN"
}

locals {
  prefix = "factoring-${var.env}"
}

# ------------------------------------------------------------------------------ AWS: data lake
resource "aws_s3_bucket" "landing" {
  bucket = "${local.prefix}-landing"
}

resource "aws_s3_bucket" "lakehouse" {
  bucket = "${local.prefix}-lakehouse"
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "all" {
  for_each = { landing = aws_s3_bucket.landing.id, lakehouse = aws_s3_bucket.lakehouse.id }
  bucket   = each.value
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "aws:kms" }
  }
}

resource "aws_s3_bucket_public_access_block" "all" {
  for_each                = { landing = aws_s3_bucket.landing.id, lakehouse = aws_s3_bucket.lakehouse.id }
  bucket                  = each.value
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Landing files are transient: once in bronze (Delta keeps history) they can expire.
resource "aws_s3_bucket_lifecycle_configuration" "landing" {
  bucket = aws_s3_bucket.landing.id
  rule {
    id     = "expire-landing"
    status = "Enabled"
    filter {}
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    expiration { days = 180 }
  }
}

# ------------------------------------------------------------------------------ Snowflake
resource "snowflake_database" "factoring" {
  name                        = upper(replace(local.prefix, "-", "_"))
  data_retention_time_in_days = 7
}

resource "snowflake_schema" "schemas" {
  for_each = toset(["RAW", "CORE", "STAGING", "MARTS", "ML", "GOVERNANCE"])
  database = snowflake_database.factoring.name
  name     = each.key
}

resource "snowflake_warehouse" "wh" {
  for_each = {
    LOAD_WH      = { size = "XSMALL", max_clusters = 1 }
    TRANSFORM_WH = { size = "SMALL", max_clusters = 1 }
    BI_WH        = { size = "SMALL", max_clusters = 3 }
  }
  name                = each.key
  warehouse_size      = each.value.size
  max_cluster_count   = each.value.max_clusters
  auto_suspend        = 60
  auto_resume         = true
  initially_suspended = true
  resource_monitor    = snowflake_resource_monitor.monthly.name
}

resource "snowflake_resource_monitor" "monthly" {
  name                      = "FACTORING_${upper(var.env)}_MONTHLY"
  credit_quota              = var.monthly_credit_quota
  frequency                 = "MONTHLY"
  start_timestamp           = "IMMEDIATELY"
  notify_triggers           = [80]
  suspend_trigger           = 100
  suspend_immediate_trigger = 110
}

resource "snowflake_account_role" "roles" {
  for_each = toset(["LOADER", "TRANSFORMER", "ANALYST", "RISK_OFFICER", "DATA_SCIENTIST"])
  name     = each.key
}

resource "snowflake_storage_integration" "lakehouse" {
  name                      = "LAKEHOUSE_S3_INT_${upper(var.env)}"
  type                      = "EXTERNAL_STAGE"
  enabled                   = true
  storage_provider          = "S3"
  storage_aws_role_arn      = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.prefix}-snowflake-reader"
  storage_allowed_locations = ["s3://${aws_s3_bucket.lakehouse.bucket}/exports/"]
}

# ------------------------------------------------------------------------------ IAM trust (Snowflake -> S3)
data "aws_caller_identity" "current" {}

resource "aws_iam_role" "snowflake_reader" {
  name = "${local.prefix}-snowflake-reader"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = snowflake_storage_integration.lakehouse.storage_aws_iam_user_arn }
      Action    = "sts:AssumeRole"
      Condition = { StringEquals = { "sts:ExternalId" = snowflake_storage_integration.lakehouse.storage_aws_external_id } }
    }]
  })
}

resource "aws_iam_role_policy" "snowflake_reader" {
  role = aws_iam_role.snowflake_reader.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:GetObjectVersion"]
        Resource = "${aws_s3_bucket.lakehouse.arn}/exports/*"
      },
      {
        Effect    = "Allow"
        Action    = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource  = aws_s3_bucket.lakehouse.arn
        Condition = { StringLike = { "s3:prefix" = ["exports/*"] } }
      }
    ]
  })
}
