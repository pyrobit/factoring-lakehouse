SCD2 history for debtors is built upstream in Databricks (`silver/debtors`, MERGE with row hash),
so no dbt snapshot is needed: `raw.debtors_history` already carries `valid_from` / `valid_to`.
A dbt `snapshot` would be the right tool if the source landed in Snowflake first. See ADR-0003.
