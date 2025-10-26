{{
    config(
        materialized='table',
        tags=['marts', 'core', 'bridge']
    )
}}

with opportunity_countries as (
    select
        opportunity_id,
        country_code,
        primary_flag,
        allocation_pct,
        role,
        created_at,
        updated_at
    from {{ ref('int_opportunity_countries_curated') }}
)

select * from opportunity_countries
