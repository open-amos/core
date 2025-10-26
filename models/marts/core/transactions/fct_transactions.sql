-- Fact table for all fund transactions
-- Thin mart layer selecting from int_transactions_classified
with transactions as (
    select
        transaction_id,
        transaction_type,
        fund_id,
        investor_id,
        instrument_id,
        investment_round_id,
        facility_id,
        name,
        description,
        amount,
        currency_code,
        fx_rate,
        amount_converted,
        fx_rate_as_of,
        fx_rate_source,
        date,
        source,
        reference,
        created_at,
        updated_at
    from {{ ref('int_transactions_classified') }}
)

select * from transactions
