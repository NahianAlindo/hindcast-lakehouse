{% macro silver_delta_scan(table_name) %}
{#
  Staging models read silver Delta tables directly from ADLS via DuckDB's
  delta_scan(), since dbt's source() can't template a table-valued function
  call. Centralized here so the storage account name (and eventually the
  Snowflake-target equivalent, once Phase 6 stands up the external stage)
  changes in exactly one place.
#}
  {%- if target.type == 'duckdb' -%}
    delta_scan('azure://silver/{{ table_name }}')
  {%- elif target.type == 'databricks' -%}
    {#
      Free Edition's serverless-only compute has no abfs/hadoop-azure
      connector and no configurable external storage location (confirmed
      live), so there's no delta_scan-equivalent in-place read here.
      warehouse/databricks/export_silver_snapshot.py +
      load_into_databricks.py COPY INTO the same three silver tables into
      workspace.hindcast_silver on a schedule the same way Phase 6's
      Snowflake COPY INTO will -- this reads that already-loaded copy.
    #}
    {{ target.catalog }}.hindcast_silver.{{ table_name }}
  {%- elif target.type == 'snowflake' -%}
    {{ exceptions.raise_compiler_error(
      "silver_delta_scan(): Snowflake target reads RAW via the external stage's "
      "COPY INTO tables (docs/PLAN.md Phase 6), not delta_scan directly -- "
      "staging models need a Snowflake-specific source() once that stage exists."
    ) }}
  {%- else -%}
    {{ exceptions.raise_compiler_error("silver_delta_scan(): unsupported target " ~ target.type) }}
  {%- endif -%}
{% endmacro %}
