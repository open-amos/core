{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimension']
    )
}}

with investors as (
    select
        investor_id,
        name,
        investor_type_id,
        created_at,
        updated_at
    from {{ ref('int_investors_unified') }}
)

select * from investors
