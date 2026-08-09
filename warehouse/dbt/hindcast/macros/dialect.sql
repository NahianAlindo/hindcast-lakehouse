{#
  Small cross-dialect shims. DuckDB and Databricks/Spark SQL agree on most
  of the SQL this project uses, but diverge on a handful of specific
  functions -- centralized here rather than scattered {% if %} blocks
  across model files, so a third target only needs one place touched.
#}

{% macro datediff_minutes(end_ts, start_ts) %}
  {%- if target.type == 'databricks' -%}
    datediff(MINUTE, {{ start_ts }}, {{ end_ts }})
  {%- else -%}
    datediff('minute', {{ start_ts }}, {{ end_ts }})
  {%- endif -%}
{% endmacro %}

{% macro datediff_seconds(end_ts, start_ts) %}
  {%- if target.type == 'databricks' -%}
    datediff(SECOND, {{ start_ts }}, {{ end_ts }})
  {%- else -%}
    datediff('second', {{ start_ts }}, {{ end_ts }})
  {%- endif -%}
{% endmacro %}

{% macro day_name(date_expr) %}
  {%- if target.type == 'databricks' -%}
    date_format({{ date_expr }}, 'EEEE')
  {%- else -%}
    strftime({{ date_expr }}, '%A')
  {%- endif -%}
{% endmacro %}

{% macro month_name(date_expr) %}
  {%- if target.type == 'databricks' -%}
    date_format({{ date_expr }}, 'MMMM')
  {%- else -%}
    strftime({{ date_expr }}, '%B')
  {%- endif -%}
{% endmacro %}

{#
  DuckDB's VARCHAR needs no length; Databricks' VARCHAR requires one, so
  this project uses STRING everywhere instead -- DuckDB accepts STRING as
  a VARCHAR alias too, making it the one spelling that works on both.
#}
{% macro string_type() %}string{% endmacro %}

{#
  UTC instant -> local wall-clock time in an IANA zone. DuckDB's
  `ts AT TIME ZONE tz` syntax isn't accepted by Databricks/Spark SQL, which
  wants the equivalent from_utc_timestamp(ts, tz) function instead.
#}
{% macro to_local_timestamp(ts_expr, tz_expr) %}
  {%- if target.type == 'databricks' -%}
    from_utc_timestamp({{ ts_expr }}, {{ tz_expr }})
  {%- else -%}
    {{ ts_expr }} at time zone {{ tz_expr }}
  {%- endif -%}
{% endmacro %}
