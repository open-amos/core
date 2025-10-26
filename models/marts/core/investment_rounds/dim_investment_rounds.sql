{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimension']
    )
}}

with investment_rounds as (
    select
        investment_round_id,
        instrument_id,
        date,
        description,
        number_of_shares_acquired,
        share_class_id,
        acquired_stake,
        created_at,
        updated_at
    from {{ ref('int_investment_rounds_curated') }}
)

select * from investment_rounds
