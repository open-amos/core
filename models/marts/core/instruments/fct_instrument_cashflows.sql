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
        fx_rate_as_of,
        fx_rate_source,
        amount_converted,
        transaction_id,
        reference,
        non_cash,
        created_at,
        updated_at
    from {{ ref('int_instrument_cashflows_curated') }}
)

select * from instrument_cashflows
