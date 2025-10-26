{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimension']
    )
}}

with share_classes as (
    select
        share_class_id,
        company_id,
        name,
        created_at,
        updated_at
    from {{ ref('int_share_classes_curated') }}
)

select * from share_classes
