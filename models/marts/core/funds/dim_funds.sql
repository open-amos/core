{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimension']
    )
}}

with funds as (
    select
        fund_id,
        name,
        type,
        vintage,
        management_fee,
        hurdle,
        carried_interest,
        target_commitment,
        incorporated_in,
        base_currency_code,
        created_at,
        updated_at
    from {{ ref('int_funds_unified') }}
)

select * from funds
