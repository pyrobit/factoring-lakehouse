{#
  Use the configured schema name as-is in prod (marts, staging, ml) instead of dbt's default
  "<target_schema>_<custom_schema>". Dev targets keep the prefix so developers never collide.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
