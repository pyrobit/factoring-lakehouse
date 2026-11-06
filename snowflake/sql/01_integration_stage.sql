-- =============================================================================================
-- 01 - Storage integration, file format and external stage over the lakehouse export bucket.
-- The integration uses an IAM role (no keys in Snowflake). Azure / GCS variants are shown below.
-- =============================================================================================
use role accountadmin;

create storage integration if not exists lakehouse_s3_int
type = external_stage
storage_provider = 's3'
enabled = true
storage_aws_role_arn = 'arn:aws:iam::123456789012:role/snowflake-factoring-export-reader'
storage_allowed_locations = ('s3://factoring-lakehouse-prod/exports/');

-- Azure ADLS Gen2:
--   create storage integration lakehouse_adls_int type = external_stage storage_provider = 'AZURE'
--     enabled = true azure_tenant_id = '<tenant-id>'
--     storage_allowed_locations = ('azure://factoringlake.blob.core.windows.net/exports/');
-- GCP GCS:
--   create storage integration lakehouse_gcs_int type = external_stage storage_provider = 'GCS'
--     enabled = true storage_allowed_locations = ('gcs://factoring-lakehouse-prod/exports/');

-- Returns the IAM user + external id to put in the AWS role trust policy (Terraform does this).
describe integration lakehouse_s3_int;

grant usage on integration lakehouse_s3_int to role loader;

use role loader;
use schema factoring.raw;

create file format if not exists parquet_ff
    type = parquet
    use_logical_type = true      -- keep DECIMAL / TIMESTAMP types written by Spark
    binary_as_text = false;

create stage if not exists lakehouse_export_stage
    storage_integration = lakehouse_s3_int
    url = 's3://factoring-lakehouse-prod/exports/'
    file_format = parquet_ff;
