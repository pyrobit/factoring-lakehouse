output "landing_bucket" {
  value = aws_s3_bucket.landing.bucket
}

output "lakehouse_bucket" {
  value = aws_s3_bucket.lakehouse.bucket
}

output "snowflake_database" {
  value = snowflake_database.factoring.name
}

output "snowflake_storage_integration" {
  value = snowflake_storage_integration.lakehouse.name
}

output "snowflake_reader_role_arn" {
  value = aws_iam_role.snowflake_reader.arn
}
