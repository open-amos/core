{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimension']
    )
}}

with instruments_equity as (
    select
        instrument_id,
        share_class_id,
        initial_share_count,
        initial_ownership_pct,
        initial_cost,
        initial_price_per_share,
        currency_code,
        fx_rate,
        fx_rate_as_of,
        fx_rate_source,
        initial_cost_converted,
        description,
        created_at,
        updated_at
    from {{ ref('int_instruments_equity_curated') }}
)

select * from instruments_equity
