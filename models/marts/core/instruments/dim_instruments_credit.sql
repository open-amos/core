{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimension']
    )
}}

with instruments_credit as (
    select
        instrument_id,
        facility_id,
        instrument_credit_type,
        tranche_label,
        commitment_amount,
        currency_code,
        fx_rate,
        fx_rate_as_of,
        fx_rate_source,
        commitment_amount_converted,
        start_date,
        maturity_date,
        interest_index,
        index_tenor_days,
        fixed_rate_pct,
        spread_bps,
        floor_pct,
        day_count,
        pay_freq_months,
        amortization_type,
        security_rank,
        status,
        created_at,
        updated_at
    from {{ ref('int_instruments_credit_curated') }}
)

select * from instruments_credit
