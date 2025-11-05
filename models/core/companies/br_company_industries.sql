{{
    config(
        materialized='table',
        tags=['marts', 'core', 'bridge']
    )
}}

with company_industries as (
    select
        company_id,
        industry_id,
        valid_from,
        valid_to,
        primary_flag,
        created_at,
        updated_at
    from {{ ref('int_company_industries_mapped') }}
)

select * from company_industries
