{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimension']
    )
}}

with loans as (
    select
        loan_id,
        instrument_id,
        facility_id,
        loan_type,
        tranche_label,
        commitment_amount,
        currency_code,
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
    from {{ ref('int_loans_curated') }}
)

select * from loans
