{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimension']
    )
}}

with industries as (
    select
        industry_id,
        canonical_name,
        created_at,
        updated_at
    from {{ ref('stg_ref__industries') }}
)

select * from industries
