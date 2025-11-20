{{
    config(
        materialized='table',
        unique_key='period_end_date',
        tags=['marts', 'metrics']
    )
}}

-- Portfolio-level aggregate performance metrics
-- Grain: One row per period_end_date

with fund_metrics as (
    select
        period_end_date,
        sum(fund_nav) as total_nav,
        sum(total_commitments) as total_commitments,
        sum(total_called_capital) as total_called_capital,
        sum(unfunded_commitment) as total_unfunded_commitment,
        sum(total_distributions) as total_distributions,
        count(distinct fund_id) as number_of_funds,
        count(distinct case when number_of_portfolio_companies > 0 then fund_id end) as funds_with_companies,
        sum(number_of_portfolio_companies) as number_of_companies,
        sum(number_of_positions) as number_of_positions
    from {{ ref('metrics_fund_performance') }}
    group by period_end_date
),

-- Calculate commitment-weighted portfolio multiples
weighted_multiples as (
    select
        period_end_date,
        -- Commitment-weighted DPI
        sum(dpi * total_commitments) / nullif(sum(total_commitments), 0) as dpi_portfolio,
        -- Commitment-weighted RVPI
        sum(rvpi * total_commitments) / nullif(sum(total_commitments), 0) as rvpi_portfolio,
        -- Commitment-weighted TVPI
        sum(tvpi * total_commitments) / nullif(sum(total_commitments), 0) as tvpi_portfolio
    from {{ ref('metrics_fund_performance') }}
    group by period_end_date
),

-- Calculate net cashflows by period
net_flows as (
    select
        date_trunc('quarter', cashflow_date) as period_end_date,
        sum(
            case 
                when direction = 'outflow' then cashflow_amount
                when direction = 'inflow' then -1 * cashflow_amount
                else 0
            end
        ) as net_cash_contributions_period
    from {{ ref('metrics_returns_cashflows') }}
    group by date_trunc('quarter', cashflow_date)
)

-- Combine all metrics
select
    fm.period_end_date,
    fm.total_nav,
    fm.total_commitments,
    fm.total_called_capital,
    fm.total_unfunded_commitment,
    fm.total_distributions,
    wm.dpi_portfolio,
    wm.rvpi_portfolio,
    wm.tvpi_portfolio,
    fm.number_of_funds,
    fm.number_of_companies,
    fm.number_of_positions,
    coalesce(nf.net_cash_contributions_period, 0) as net_cash_contributions_period,
    current_date as as_of_date
from fund_metrics fm
left join weighted_multiples wm on fm.period_end_date = wm.period_end_date
left join net_flows nf on fm.period_end_date = nf.period_end_date
order by fm.period_end_date
