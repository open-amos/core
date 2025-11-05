{{
    config(
        materialized='table',
        tags=['marts', 'core', 'bridge']
    )
}}

with instrument_countries as (
    select
        instrument_id,
        country_code,
        valid_from,
        valid_to,
        allocation_pct,
        role,
        primary_flag,
        created_at,
        updated_at
    from {{ ref('int_instrument_countries_mapped') }}
)

select * from instrument_countries
