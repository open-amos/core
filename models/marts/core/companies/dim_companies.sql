{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimension']
    )
}}

with companies as (
    select
        company_id,
        name,
        website,
        description,
        currency_code,
        created_at,
        updated_at
    from {{ ref('int_companies_unified') }}
)

select * from companies
