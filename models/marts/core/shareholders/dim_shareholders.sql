{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimension']
    )
}}

with shareholders as (
    select
        shareholder_id,
        company_id,
        shareholder_name as name,
        type,
        number_of_shares,
        share_class_id,
        affiliated_entity,
        created_at,
        updated_at
    from {{ ref('int_shareholders_curated') }}
)

select * from shareholders
