{{
    config(
        materialized='table',
        tags=['marts', 'core', 'bridge']
    )
}}

with instrument_industries as (
    select
        instrument_id,
        industry_id,
        valid_from,
        valid_to,
        allocation_pct,
        primary_flag,
        created_at,
        updated_at
    from {{ ref('int_instrument_industries_mapped') }}
)

select * from instrument_industries
