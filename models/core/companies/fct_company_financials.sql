-- Fact table for company financial statements
-- Thin mart layer selecting from int_company_financials_curated
with company_financials as (
    select
        company_performance_snapshot_id,
        company_id,
        period_start_date,
        period_end_date,
        frequency,
        reporting_basis,
        snapshot_source,
        currency_code,
        revenue,
        cost_of_goods_sold,
        gross_profit,
        operating_expenses,
        ebitda,
        depreciation_amortization,
        ebit,
        net_income,
        cash,
        total_assets,
        total_liabilities,
        equity,
        operating_cash_flow,
        investing_cash_flow,
        financing_cash_flow,
        source_system,
        source_file_ref,
        created_at,
        updated_at
    from {{ ref('int_company_financials_curated') }}
)

select * from company_financials
