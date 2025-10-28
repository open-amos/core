{% macro compat_dateadd(datepart, interval, date_expr) %}
{#
Cross-database dateadd helper.
Usage: {{ compat_dateadd('month', -3, 'cm.period_end_date') }}
Supports Postgres/Redshift and Snowflake. Falls back to Snowflake syntax otherwise.
#}

{# normalize inputs #}
{%- set unit = (datepart | lower) -%}
{%- set n = interval -%}
{%- set expr = date_expr -%}
{# if expr is quoted like 'cm.period_end_date', strip quotes #}
{%- if expr is string and expr | length >= 2 and expr[0] == "'" and expr[-1] == "'" -%}
  {%- set expr = expr[1:-1] -%}
{%- endif -%}

{%- if target.type in ['postgres', 'redshift'] -%}
  {%- if unit in ['day', 'days'] -%}
    {{ expr }} + ({{ n }}) * interval '1 day'
  {%- elif unit in ['month', 'months'] -%}
    {{ expr }} + ({{ n }}) * interval '1 month'
  {%- elif unit in ['year', 'years'] -%}
    {{ expr }} + ({{ n }}) * interval '1 year'
  {%- elif unit in ['hour', 'hours'] -%}
    {{ expr }} + ({{ n }}) * interval '1 hour'
  {%- elif unit in ['minute', 'minutes'] -%}
    {{ expr }} + ({{ n }}) * interval '1 minute'
  {%- elif unit in ['second', 'seconds'] -%}
    {{ expr }} + ({{ n }}) * interval '1 second'
  {%- else -%}
    {{ expr }} + ({{ n }}) * interval '1 day'
  {%- endif -%}
{%- elif target.type == 'snowflake' -%}
  dateadd({{ unit }}, {{ n }}, {{ expr }})
{%- else -%}
  dateadd({{ unit }}, {{ n }}, {{ expr }})
{%- endif -%}
{% endmacro %}


