-- Fact table for fund performance snapshots
-- Thin mart layer selecting from int_fund_snapshots_curated
with fund_snapshots as (
    select
        fund_snapshot_id,
        fund_id,
        period_start_date,
        period_end_date,
        frequency,
        reporting_basis,
        snapshot_source,
        committed_capital,
        called_capital,
        dpi,
        rvpi,
        expected_coc,
        cash_amount,
        total_distributions,
        total_expenses,
        total_management_fees,
        total_loans_received,
        principal_outstanding,
        undrawn_commitment,
        total_interest_income,
        created_at,
        updated_at
    from {{ ref('int_fund_snapshots_curated') }}
)

select * from fund_snapshots
