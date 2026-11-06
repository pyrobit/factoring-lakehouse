{# Sort order for aging buckets in BI tools. #}
{% macro aging_bucket_order(bucket_col) -%}
    case {{ bucket_col }}
        when 'CURRENT' then 0
        when '1-30' then 1
        when '31-60' then 2
        when '61-90' then 3
        when '90+' then 4
        else 9
    end
{%- endmacro %}

{# Divide safely, returning null on a zero denominator (Snowflake div0 returns 0, which hides problems). #}
{% macro safe_divide(numerator, denominator) -%}
    ({{ numerator }}) / nullif({{ denominator }}, 0)
{%- endmacro %}

{# Attach the client row-access policy to a model that has a client_id column. #}
{% macro apply_client_rap() -%}
    {% if target.name == 'prod' %}
        alter table {{ this }} add row access policy factoring.governance.client_portfolio_rap on (client_id)
    {% else %}
        select 1
    {% endif %}
{%- endmacro %}
