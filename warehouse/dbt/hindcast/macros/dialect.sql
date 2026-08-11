-- Small cross-dialect shims. DuckDB, Databricks/Spark SQL, and Snowflake
-- agree on most of the SQL this project uses, but diverge on a handful of
-- specific functions -- centralized here rather than scattered per-model
-- conditionals across model files, so a new target only needs one place
-- touched. DuckDB is the implicit "else" branch throughout since it was the
-- first target built and every macro started as DuckDB-only syntax.

{% macro is_databricks() %}
  {{ return(target.type == 'databricks') }}
{% endmacro %}

{% macro is_snowflake() %}
  {{ return(target.type == 'snowflake') }}
{% endmacro %}

{% macro datediff_minutes(end_ts, start_ts) %}
  {%- if is_databricks() or is_snowflake() -%}
    datediff(minute, {{ start_ts }}, {{ end_ts }})
  {%- else -%}
    datediff('minute', {{ start_ts }}, {{ end_ts }})
  {%- endif -%}
{% endmacro %}

{% macro datediff_seconds(end_ts, start_ts) %}
  {%- if is_databricks() or is_snowflake() -%}
    datediff(second, {{ start_ts }}, {{ end_ts }})
  {%- else -%}
    datediff('second', {{ start_ts }}, {{ end_ts }})
  {%- endif -%}
{% endmacro %}

{% macro day_name(date_expr) %}
  {%- if is_databricks() -%}
    date_format({{ date_expr }}, 'EEEE')
  {%- elif is_snowflake() -%}
    -- Snowflake's TO_CHAR 'Day' format model is blank-padded to 9 chars
    -- ('Monday   ') -- TRIM, not a separate width-aware substring.
    trim(to_char({{ date_expr }}, 'Day'))
  {%- else -%}
    strftime({{ date_expr }}, '%A')
  {%- endif -%}
{% endmacro %}

{% macro month_name(date_expr) %}
  {%- if is_databricks() -%}
    date_format({{ date_expr }}, 'MMMM')
  {%- elif is_snowflake() -%}
    trim(to_char({{ date_expr }}, 'Month'))
  {%- else -%}
    strftime({{ date_expr }}, '%B')
  {%- endif -%}
{% endmacro %}

-- DuckDB's VARCHAR needs no length; Databricks' VARCHAR requires one, so
-- this project uses STRING everywhere instead -- DuckDB accepts STRING as
-- a VARCHAR alias too, making it the one spelling that works on both.
-- Snowflake also accepts STRING as a built-in VARCHAR synonym, so this one
-- stays a single spelling across all three targets.
{% macro string_type() %}
  string
{% endmacro %}

-- UTC instant -> local wall-clock time in an IANA zone. DuckDB's
-- `ts AT TIME ZONE tz` syntax isn't accepted by Databricks/Spark SQL
-- (wants from_utc_timestamp(ts, tz)) or by Snowflake (wants
-- convert_timezone('UTC', tz, ts) -- Snowflake's 2-arg CONVERT_TIMEZONE
-- assumes the source is already TIMESTAMP_LTZ, which these naive UTC
-- timestamps aren't, so the source zone must be given explicitly).
{% macro to_local_timestamp(ts_expr, tz_expr) %}
  {%- if is_databricks() -%}
    from_utc_timestamp({{ ts_expr }}, {{ tz_expr }})
  {%- elif is_snowflake() -%}
    convert_timezone('UTC', {{ tz_expr }}, {{ ts_expr }})
  {%- else -%}
    {{ ts_expr }} at time zone {{ tz_expr }}
  {%- endif -%}
{% endmacro %}

-- Row source for dim_time: hours 0-23. DuckDB and Spark SQL both support
-- range() as a table-valued function aliased `AS t(h)`; Snowflake has no
-- equivalent and instead generates N rows via TABLE(GENERATOR(...)) +
-- SEQ4() (0-indexed per row, exactly matching what's needed here).
{% macro hour_series() %}
  {%- if is_snowflake() -%}
    (select seq4() as h from table(generator(rowcount => 24))) as t
  {%- else -%}
    range(0, 24) as t(h)
  {%- endif -%}
{% endmacro %}

-- Row source for dim_date: one row per day in [start_date, end_date) --
-- end_date is always EXCLUSIVE, matching DuckDB's native range() semantics
-- (the "else" branch); the other two branches adjust internally rather than
-- pushing that adjustment onto every call site. start_date/end_date are raw
-- SQL fragments the caller must quote itself (e.g. "'2025-01-01'").
--
-- DuckDB's range() accepts date bounds directly. Spark SQL's range() is
-- integer-only (no date bounds) -- sequence()+explode() is the Spark-native
-- date-series generator, but sequence()'s end bound is INCLUSIVE, so this
-- branch subtracts a day to match the shared exclusive contract. Snowflake
-- has neither range() nor sequence(); TABLE(GENERATOR(...)) + SEQ4() is its
-- row-generation idiom, and GENERATOR's ROWCOUNT needs a fixed literal (no
-- dynamic-expression form verified), so this over-generates and filters
-- down with a WHERE rather than computing an exact day count at compile time.
-- Returns a complete query (a CTE body, not a bare table-reference snippet
-- like hour_series() above) outputting one `date_day` column -- Databricks'
-- already-verified shape uses explode() as a column-list generator with no
-- table reference at all, which a table-reference-only macro can't represent
-- without guessing at Spark's newer (unverified here) row-generator syntax,
-- so this macro swaps the whole query instead, same as the original
-- per-model target.type conditional block it replaces.
{% macro date_series(start_date, end_date) %}
  {%- if is_snowflake() -%}
    select d as date_day from (
      select dateadd(day, seq4(), cast({{ start_date }} as date)) as d
      from table(generator(rowcount => 3000))
    ) gen
    where d < cast({{ end_date }} as date)
  {%- elif is_databricks() -%}
    select explode(sequence(
        cast({{ start_date }} as date),
        date_sub(cast({{ end_date }} as date), 1),
        interval 1 day
    )) as date_day
  {%- else -%}
    select cast(d as date) as date_day
    from range(cast({{ start_date }} as date), cast({{ end_date }} as date), interval 1 day) as t(d)
  {%- endif -%}
{% endmacro %}
