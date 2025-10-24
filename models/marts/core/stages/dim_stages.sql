{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimension']
    )
}}

with stages as (
    select
        stage_id,
        name,
        pipeline_type as type,
        stage_order as "order",
        loaded_at as created_at,
        loaded_at as updated_at
    from {{ ref('stg_crm__stages') }}
)

select * from stages
