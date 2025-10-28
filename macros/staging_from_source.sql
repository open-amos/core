{% macro staging_from_source(source_name, table_name) %}
{#
YAML-driven staging macro that generates staging models from source tables
with optional column mappings and transformations defined in source metadata.

Usage:
    {{ amos_core.staging_from_source('crm_vendor', 'amos_crm_companies') }}

Supports metadata configuration in sources.yml:
- column_mappings: Map source columns to target columns
- column_transformations: Apply transformations (trim, upper, cast, etc.)
- exclude_columns: List of columns to exclude
- include_columns: List of columns to include (if specified, only these are included)
- add_metadata: Boolean to add source system metadata columns (default: true)
#}

{%- set source_relation = source(source_name, table_name) -%}

{# During parse phase, return a simple placeholder query #}
{%- if not execute -%}
    select
        *,
        '{{ source_name | upper }}' as source_system,
        '{{ table_name }}' as source_table,
        current_timestamp as loaded_at
    from {{ source_relation }}
{%- else -%}

{# Find source metadata #}
{%- set source_meta = graph.sources.values() | selectattr('source_name', 'equalto', source_name) | selectattr('name', 'equalto', table_name) | first -%}
{%- set meta = source_meta.meta if source_meta else {} -%}

{# Get configuration from metadata #}
{%- set column_mappings = meta.get('column_mappings', {}) -%}
{%- set column_transformations = meta.get('column_transformations', {}) -%}
{%- set exclude_columns = meta.get('exclude_columns', []) -%}
{%- set include_columns = meta.get('include_columns', []) -%}
{%- set add_metadata = meta.get('add_metadata', true) -%}

{# Get source columns #}
{%- set source_columns = adapter.get_columns_in_relation(source_relation) -%}

select
{%- set filtered_columns = [] -%}
{%- for column in source_columns -%}
    {%- set col_name = column.name | lower -%}
    {%- if col_name not in exclude_columns -%}
        {%- if include_columns | length == 0 or col_name in include_columns -%}
            {%- set _ = filtered_columns.append(column) -%}
        {%- endif -%}
    {%- endif -%}
{%- endfor %}
{%- for column in filtered_columns -%}
    {%- set col_name = column.name | lower -%}

    {# Apply transformation if specified #}
    {%- if col_name in column_transformations -%}
        {%- set transformation = column_transformations[col_name] %}
    {{ transformation }} as {{ column_mappings.get(col_name, col_name) }}
    {%- else -%}
        {# Apply default transformation (trim for strings) #}
        {%- if column.dtype in ('TEXT', 'VARCHAR', 'STRING', 'CHAR') %}
    trim({{ col_name }}) as {{ column_mappings.get(col_name, col_name) }}
        {%- else %}
    {{ col_name }} as {{ column_mappings.get(col_name, col_name) }}
        {%- endif -%}
    {%- endif -%}
    {%- if not loop.last %},{% endif %}
{%- endfor %}

{%- if add_metadata %}
    {%- set col_names_lower = source_columns | map(attribute='name') | map('lower') | list %}
    {%- set meta_exprs = [] -%}
    {%- if 'source_system' not in col_names_lower -%}
        {%- set _ = meta_exprs.append("'" ~ source_name | upper ~ "' as source_system") -%}
    {%- endif -%}
    {%- if 'source_table' not in col_names_lower -%}
        {%- set _ = meta_exprs.append("'" ~ table_name ~ "' as source_table") -%}
    {%- endif -%}
    {%- if 'loaded_at' not in col_names_lower -%}
        {%- set _ = meta_exprs.append("current_timestamp as loaded_at") -%}
    {%- endif -%}
    {%- if meta_exprs | length > 0 -%}
        {%- if filtered_columns | length > 0 -%}
,
        {%- endif -%}
-- Source system metadata
{{ meta_exprs | join(',\n') }}
    {%- endif -%}
{%- endif %}

from {{ source_relation }}

{%- endif -%}
{% endmacro %}
