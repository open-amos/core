{{
    config(
        materialized='table',
        tags=['marts', 'core', 'bridge']
    )
}}

with opportunity_industries as (
    select
        opportunity_id,
        industry_id,
        primary_flag,
        created_at,
        updated_at
    from {{ ref('int_opportunity_industries_curated') }}
)

select * from opportunity_industries
