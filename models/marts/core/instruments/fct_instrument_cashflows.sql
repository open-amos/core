-- Fact table for instrument-level cash movements
-- Thin mart layer selecting from int_instrument_cashflows_curated
with instrument_cashflows as (
    select
        instrument_cashflow_id,
        instrument_id,
        instrument_cashflow_type,
        date,
        amount,
        currency_code,
        fx_rate,
        amount_converted,
        transaction_id,
        reference,
        created_at,
        updated_at
    from {{ ref('int_instrument_cashflows_curated') }}
)

select * from instrument_cashflows
