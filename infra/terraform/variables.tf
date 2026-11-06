variable "env" {
  description = "Deployment environment (dev | prod)"
  type        = string
  validation {
    condition     = contains(["dev", "prod"], var.env)
    error_message = "env must be dev or prod."
  }
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "snowflake_org" {
  type = string
}

variable "snowflake_account" {
  type = string
}

variable "snowflake_user" {
  description = "Terraform service user (key-pair auth)"
  type        = string
}

variable "snowflake_private_key_path" {
  type      = string
  sensitive = true
}

variable "monthly_credit_quota" {
  type    = number
  default = 200
}
