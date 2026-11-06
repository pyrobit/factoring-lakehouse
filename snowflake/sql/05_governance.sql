-- =============================================================================================
-- 05 - Governance: PII tag + tag-based masking, row-access policy per client portfolio, and
-- an audit query. Policies live in their own schema and are owned by a governance role.
-- =============================================================================================
use role sysadmin;
use schema factoring.governance;

create tag if not exists pii allowed_values 'tax_id', 'name', 'email'
comment = 'Personally identifiable / sensitive business identifiers';

create masking policy if not exists mask_pii_string as (val string) returns string -> -- noqa: LT01
case
    when is_role_in_session('RISK_OFFICER') then val
    when is_role_in_session('LOADER') or is_role_in_session('TRANSFORMER') then val
    else regexp_replace(val, '[0-9]', '*')
end
comment = 'Unmasked for risk officers and pipelines; digits masked for everyone else';

-- Tag-based masking: any column tagged PII gets the policy automatically, including new ones.
alter tag pii set masking policy mask_pii_string;

alter table factoring.raw.debtors_history modify column tax_id set tag pii = 'tax_id';

-- Row access: account managers only see the clients they manage.
create table if not exists client_entitlements (
    role_name varchar not null,
    client_id varchar not null
);

-- The argument is prefixed p_ so it can never be confused with the entitlement table's column.
create row access policy if not exists client_portfolio_rap as (p_client_id varchar) returns boolean -> -- noqa: LT01
is_role_in_session('RISK_OFFICER')
or is_role_in_session('TRANSFORMER')
or exists (
    select 1
    from factoring.governance.client_entitlements as e
    where e.client_id = p_client_id and is_role_in_session(e.role_name) -- noqa: RF02, RF03
);

-- Applied to the marts by dbt post-hooks (see dbt/models/marts/_marts.yml).

-- Audit: who read debtor PII in the last 7 days (needs ACCOUNT_USAGE access).
select
    ah.query_start_time,
    ah.user_name,
    obj.value:"objectName"::string as object_name,
    col.value:"columnName"::string as column_name
from snowflake.account_usage.access_history as ah,
    lateral flatten(input => ah.base_objects_accessed) as obj,
    lateral flatten(input => obj.value:"columns") as col
where
    ah.query_start_time >= dateadd(day, -7, current_timestamp())
    and col.value:"columnName"::string = 'TAX_ID'
order by ah.query_start_time desc;
