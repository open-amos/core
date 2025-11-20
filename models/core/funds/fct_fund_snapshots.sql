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
        currency_code,
        committed_capital,
        called_capital,
        dpi,
        rvpi,
        expected_coc,
        cash_amount,
        total_distributions,
        total_expenses,
        total_management_fees,
        total_interest_income,
        coalesce(nav_amount, null) as nav_amount,
        coalesce(nav_amount_converted, null) as nav_amount_converted,
        source,
        source_reference,
        created_at,
        updated_at
    from {{ ref('int_fund_snapshots_curated') }}
)

select * from fund_snapshots
