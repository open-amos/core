-- Fact table for loan-level cash movements
-- Thin mart layer selecting from int_loan_cashflows_curated
with loan_cashflows as (
    select
        loan_cashflow_id,
        loan_id,
        loan_cashflow_type,
        date,
        amount,
        currency_code,
        fx_rate,
        amount_converted,
        interest_period_id,
        transaction_id,
        reference,
        created_at,
        updated_at
    from {{ ref('int_loan_cashflows_curated') }}
)

select * from loan_cashflows
