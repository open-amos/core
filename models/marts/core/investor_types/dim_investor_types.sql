{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimension']
    )
}}

with investor_types as (
    select
        investor_type_id,
        name,
        description,
        kyc_category,
        current_timestamp as created_at,
        current_timestamp as updated_at
    from {{ ref('stg_ref__investor_types') }}
)

select * from investor_types
