-- Fact table for loan interest accrual periods
-- Thin mart layer selecting from int_loan_interest_periods_curated
with loan_interest_periods as (
    select
        loan_interest_period_id,
        loan_id,
        period_start,
        period_end,
        index_rate_pct,
        margin_pct,
        accrual_days,
        expected_interest_amount,
        payment_due_date,
        actual_interest_amount,
        created_at,
        updated_at
    from {{ ref('int_loan_interest_periods_curated') }}
)

select * from loan_interest_periods
