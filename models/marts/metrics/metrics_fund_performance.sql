{{
    config(
        materialized='table',
        unique_key=['fund_id', 'period_end_date'],
        tags=['marts', 'metrics', 'ilpa']
    )
}}

-- Fund-level performance metrics aligned with ILPA reporting standards
-- Grain: One row per fund per period_end_date

with funds as (
    select
        fund_id,
        name as fund_name
    from {{ ref('dim_funds') }}
),

fund_snapshots as (
    select
        fund_id,
        period_end_date,
        committed_capital as total_commitments,
        called_capital as total_called_capital,
        dpi,
        rvpi,
        expected_coc,
        cash_amount,
        total_distributions,
        total_interest_income as interest_income
    from {{ ref('fct_fund_snapshots') }}
),

-- Get all instruments for each fund
instruments as (
    select
        instrument_id,
        fund_id,
        company_id,
        termination_date
    from {{ ref('dim_instruments') }}
),

-- Calculate sum of instrument fair values per fund per period
instrument_valuations as (
    select
        i.fund_id,
        s.period_end_date,
        sum(s.fair_value_converted) as total_instrument_fair_value
    from {{ ref('fct_instrument_snapshots') }} s
    inner join instruments i on s.instrument_id = i.instrument_id
    group by i.fund_id, s.period_end_date
),

-- Count portfolio companies (distinct companies with active instruments)
portfolio_company_counts as (
    select
        i.fund_id,
        s.period_end_date,
        count(distinct i.company_id) as number_of_portfolio_companies
    from {{ ref('fct_instrument_snapshots') }} s
    inner join instruments i on s.instrument_id = i.instrument_id
    where i.termination_date is null  -- Only active instruments
        and i.company_id is not null
    group by i.fund_id, s.period_end_date
),

-- Count positions (distinct active instruments)
position_counts as (
    select
        i.fund_id,
        s.period_end_date,
        count(distinct i.instrument_id) as number_of_positions
    from {{ ref('fct_instrument_snapshots') }} s
    inner join instruments i on s.instrument_id = i.instrument_id
    where i.termination_date is null  -- Only active instruments
    group by i.fund_id, s.period_end_date
),

-- Calculate lines of credit outstanding from credit instrument snapshots
credit_exposure as (
    select
        i.fund_id,
        s.period_end_date,
        sum(sc.principal_outstanding_converted) as lines_of_credit_outstanding
    from {{ ref('fct_instrument_snapshots') }} s
    inner join {{ ref('fct_instrument_snapshots_credit') }} sc
        on s.instrument_snapshot_id = sc.instrument_snapshot_id
    inner join instruments i on s.instrument_id = i.instrument_id
    group by i.fund_id, s.period_end_date
),

-- Combine all metrics
fund_metrics as (
    select
        f.fund_id,
        f.fund_name,
        fs.period_end_date,
        -- Calculate fund NAV: sum of instrument fair values + fund cash
        coalesce(iv.total_instrument_fair_value, 0) + coalesce(fs.cash_amount, 0) as fund_nav,
        fs.total_commitments,
        fs.total_called_capital,
        -- Calculate unfunded commitment
        fs.total_commitments - fs.total_called_capital as unfunded_commitment,
        fs.total_distributions,
        fs.dpi,
        fs.rvpi,
        -- Calculate TVPI as DPI + RVPI
        fs.dpi + fs.rvpi as tvpi,
        fs.expected_coc,
        coalesce(pcc.number_of_portfolio_companies, 0) as number_of_portfolio_companies,
        coalesce(pc.number_of_positions, 0) as number_of_positions,
        coalesce(ce.lines_of_credit_outstanding, 0) as lines_of_credit_outstanding,
        fs.interest_income,
        current_date as as_of_date
    from funds f
    inner join fund_snapshots fs on f.fund_id = fs.fund_id
    left join instrument_valuations iv 
        on f.fund_id = iv.fund_id 
        and fs.period_end_date = iv.period_end_date
    left join portfolio_company_counts pcc 
        on f.fund_id = pcc.fund_id 
        and fs.period_end_date = pcc.period_end_date
    left join position_counts pc 
        on f.fund_id = pc.fund_id 
        and fs.period_end_date = pc.period_end_date
    left join credit_exposure ce 
        on f.fund_id = ce.fund_id 
        and fs.period_end_date = ce.period_end_date
),

-- Calculate peak outstanding credit using window function
final_metrics as (
    select
        fund_id,
        fund_name,
        period_end_date,
        fund_nav,
        total_commitments,
        total_called_capital,
        unfunded_commitment,
        total_distributions,
        dpi,
        rvpi,
        tvpi,
        expected_coc,
        number_of_portfolio_companies,
        number_of_positions,
        lines_of_credit_outstanding,
        -- Calculate peak outstanding credit with window function
        max(lines_of_credit_outstanding) over (
            partition by fund_id 
            order by period_end_date 
            rows between unbounded preceding and current row
        ) as peak_outstanding_credit,
        interest_income,
        as_of_date
    from fund_metrics
)

select * from final_metrics
