-- =============================================================================================
-- 00 - Account setup: RBAC hierarchy, warehouses, cost guardrails, database and schemas.
-- Run as ACCOUNTADMIN once (Terraform in infra/terraform manages the same objects declaratively).
-- =============================================================================================
use role securityadmin;

-- Functional roles (granted to people / service users) ----------------------------------------
create role if not exists loader comment = 'Service role: Snowpipe, COPY INTO, raw merges';
create role if not exists transformer comment = 'Service role: dbt builds';
create role if not exists analyst comment = 'Read marts; PII masked';
create role if not exists risk_officer comment = 'Read marts including unmasked debtor PII';
create role if not exists data_scientist comment = 'Read feature tables';

-- Role hierarchy rolls up to SYSADMIN so admins can manage every object.
grant role loader to role sysadmin;
grant role transformer to role sysadmin;
grant role analyst to role transformer;
grant role risk_officer to role sysadmin;
grant role data_scientist to role sysadmin;

use role sysadmin;

-- Warehouses: separate per workload, so loads never queue behind BI and costs are attributable.
create warehouse if not exists load_wh
warehouse_size = xsmall
auto_suspend = 60
auto_resume = true
initially_suspended = true
comment = 'COPY / MERGE from the lakehouse export';

create warehouse if not exists transform_wh
warehouse_size = small
auto_suspend = 60
auto_resume = true
initially_suspended = true
comment = 'dbt builds';

create warehouse if not exists bi_wh
warehouse_size = small
min_cluster_count = 1
max_cluster_count = 3
scaling_policy = standard
auto_suspend = 300
auto_resume = true
initially_suspended = true
comment = 'BI dashboards; multi-cluster for concurrency';

-- Cost guardrail: notify at 80%, suspend at 100% of the monthly credit quota.
use role accountadmin;
create resource monitor if not exists factoring_monthly
with credit_quota = 200
frequency = monthly
start_timestamp = immediately
triggers on 80 percent do notify
on 100 percent do suspend
on 110 percent do suspend_immediate;

alter warehouse load_wh set resource_monitor = factoring_monthly;
alter warehouse transform_wh set resource_monitor = factoring_monthly;
alter warehouse bi_wh set resource_monitor = factoring_monthly;

use role sysadmin;
create database if not exists factoring data_retention_time_in_days = 7;
create schema if not exists factoring.raw comment = 'Landed from the lakehouse export (1:1 with Delta)';
create schema if not exists factoring.core comment = 'Incrementally merged by streams + tasks';
create schema if not exists factoring.staging comment = 'dbt staging views';
create schema if not exists factoring.marts comment = 'dbt marts for BI';
create schema if not exists factoring.ml comment = 'Feature tables for data science';
create schema if not exists factoring.governance comment = 'Policies and tags';

-- Grants -------------------------------------------------------------------------------------
grant usage on warehouse load_wh to role loader;
grant usage on warehouse transform_wh to role transformer;
grant usage on warehouse bi_wh to role analyst;
grant usage on warehouse bi_wh to role risk_officer;
grant usage on warehouse bi_wh to role data_scientist;

grant usage on database factoring to role loader;
grant usage on database factoring to role transformer;
grant usage on database factoring to role analyst;
grant usage on database factoring to role risk_officer;
grant usage on database factoring to role data_scientist;

grant all on schema factoring.raw to role loader;
grant all on schema factoring.core to role loader;
grant usage on schema factoring.raw to role transformer;
grant select on future tables in schema factoring.raw to role transformer;
grant select on future tables in schema factoring.core to role transformer;
grant all on schema factoring.staging to role transformer;
grant all on schema factoring.marts to role transformer;
grant all on schema factoring.ml to role transformer;

grant usage on schema factoring.marts to role analyst;
grant select on future tables in schema factoring.marts to role analyst;
grant select on future views in schema factoring.marts to role analyst;
grant usage on schema factoring.marts to role risk_officer;
grant select on future tables in schema factoring.marts to role risk_officer;
grant usage on schema factoring.ml to role data_scientist;
grant select on future tables in schema factoring.ml to role data_scientist;
