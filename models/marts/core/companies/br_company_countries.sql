{{
    config(
        materialized='table',
        tags=['marts', 'core', 'bridge']
    )
}}

with company_countries as (
    select
        company_id,
        country_code,
        valid_from,
        valid_to,
        allocation_pct,
        role,
        primary_flag,
        created_at,
        updated_at
    from {{ ref('int_company_countries_mapped') }}
)

select * from company_countries
