{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimension']
    )
}}

with opportunities as (
    select
        opportunity_id,
        fund_id,
        name,
        stage_id,
        company_id,
        responsible,
        amount,
        source,
        close_date,
        created_at,
        updated_at
    from {{ ref('int_opportunities_curated') }}
)

select * from opportunities
