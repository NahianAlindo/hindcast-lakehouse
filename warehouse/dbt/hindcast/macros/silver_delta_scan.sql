-- Staging models read silver Delta tables directly from ADLS via DuckDB's
-- delta_scan(), since dbt's source() can't template a table-valued function
-- call. Centralized here so the storage account name (and eventually the
-- Snowflake-target equivalent, once Phase 6 stands up the external stage)
-- changes in exactly one place.
{% macro silver_delta_scan(table_name) %}
  {%- if target.type == 'duckdb' -%}
    delta_scan('azure://silver/{{ table_name }}')
  {%- elif is_databricks() -%}
    -- Free Edition's serverless-only compute has no abfs/hadoop-azure
    -- connector and no configurable external storage location (confirmed
    -- live), so there's no delta_scan-equivalent in-place read here.
    -- warehouse/databricks/export_silver_snapshot.py +
    -- load_into_databricks.py COPY INTO the same three silver tables into
    -- workspace.hindcast_silver on a schedule the same way Phase 6's
    -- Snowflake COPY INTO will -- this reads that already-loaded copy.
    {{ target.catalog }}.hindcast_silver.{{ table_name }}
  {%- elif target.type == 'snowflake' -%}
    -- warehouse/snowflake/load_into_snowflake.py COPY INTOs the same three
    -- silver tables (same export_silver_snapshot.py pre-step as Databricks,
    -- different landing spot) into HINDCAST.RAW on the silver_to_snowflake
    -- DAG's @hourly cadence -- this reads that already-loaded copy, exactly
    -- like the Databricks branch above reads workspace.hindcast_silver.
    {{ target.database }}.RAW.{{ table_name }}
  {%- else -%}
    {{ exceptions.raise_compiler_error("silver_delta_scan(): unsupported target " ~ target.type) }}
  {%- endif -%}
{% endmacro %}
