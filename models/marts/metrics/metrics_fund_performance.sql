{{
    config(
        materialized='table',
        unique_key=['fund_id', 'period_end_date'],
        tags=['marts', 'metrics']
    )
}}

-- Fund-level performance metrics
-- Grain: One row per fund per period_end_date

with funds as (
    select
        fund_id,
        name as fund_name,
        type as fund_type
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
        total_interest_income as interest_income,
        nav_amount_converted
    from {{ ref('fct_fund_snapshots') }}
),

-- Get all instruments for each fund
instruments as (
    select
        instrument_id,
        fund_id,
        company_id,
        instrument_type,
        inception_date,
        termination_date
    from {{ ref('dim_instruments') }}
),

-- Calculate sum of instrument fair values per fund per period (equity instruments only)
instrument_valuations as (
    select
        i.fund_id,
        s.period_end_date,
        sum(s.fair_value_converted) as total_instrument_fair_value
    from {{ ref('fct_instrument_snapshots') }} s
    inner join instruments i on s.instrument_id = i.instrument_id
    where i.instrument_type = 'EQUITY'
    group by i.fund_id, s.period_end_date
),

-- Count portfolio companies (distinct companies with active instruments at each period)
portfolio_company_counts as (
    select
        fs.fund_id,
        fs.period_end_date,
        count(distinct case when i.company_id is not null then i.company_id end) as number_of_portfolio_companies
    from fund_snapshots fs
    left join instruments i 
        on fs.fund_id = i.fund_id
        and i.company_id is not null
        and i.inception_date <= fs.period_end_date  -- Instrument existed at this period
        and (i.termination_date is null or i.termination_date > fs.period_end_date)  -- Still active at this period
    group by fs.fund_id, fs.period_end_date
),

-- Count positions (distinct active instruments at each period)
position_counts as (
    select
        fs.fund_id,
        fs.period_end_date,
        count(distinct case when i.instrument_id is not null then i.instrument_id end) as number_of_positions
    from fund_snapshots fs
    left join instruments i 
        on fs.fund_id = i.fund_id
        and i.inception_date <= fs.period_end_date  -- Instrument existed at this period
        and (i.termination_date is null or i.termination_date > fs.period_end_date)  -- Still active at this period
    group by fs.fund_id, fs.period_end_date
),

-- Calculate period net flows (contributions minus distributions)
period_net_flows as (
    select
        i.fund_id,
        (date_trunc('quarter', cf.date) + interval '3 months - 1 day')::date as period_end_date,
        sum(
            case 
                when cf.instrument_cashflow_type in ('CONTRIBUTION', 'PURCHASE', 'DRAW') 
                then cf.amount_converted
                when cf.instrument_cashflow_type in ('DISTRIBUTION', 'DIVIDEND', 'SALE', 'PRINCIPAL', 'PREPAYMENT')
                then -1 * cf.amount_converted
                else 0
            end
        ) as period_net_flows
    from {{ ref('fct_instrument_cashflows') }} cf
    inner join instruments i on cf.instrument_id = i.instrument_id
    group by i.fund_id, (date_trunc('quarter', cf.date) + interval '3 months - 1 day')::date
),

-- Calculate lines of credit outstanding from credit instrument snapshots
credit_exposure as (
    select
        i.fund_id,
        s.period_end_date,
        sum(sc.principal_outstanding_converted) as lines_of_credit_outstanding,
        sum(sc.principal_outstanding_converted) as total_principal_outstanding,
        sum(sc.undrawn_commitment_converted) as total_undrawn_commitment,
        sum(sc.principal_outstanding_converted) + sum(sc.undrawn_commitment_converted) as total_credit_exposure
    from {{ ref('fct_instrument_snapshots') }} s
    inner join {{ ref('fct_instrument_snapshots_credit') }} sc
        on s.instrument_snapshot_id = sc.instrument_snapshot_id
    inner join instruments i on s.instrument_id = i.instrument_id
    group by i.fund_id, s.period_end_date
),

-- Calculate total interest income from INTEREST cashflows for credit funds
credit_interest_income as (
    select
        i.fund_id,
        (date_trunc('quarter', cf.date) + interval '3 months - 1 day')::date as period_end_date,
        sum(cf.amount_converted) as total_interest_income
    from {{ ref('fct_instrument_cashflows') }} cf
    inner join instruments i on cf.instrument_id = i.instrument_id
    where cf.instrument_cashflow_type = 'INTEREST'
        and i.instrument_type = 'CREDIT'
    group by i.fund_id, (date_trunc('quarter', cf.date) + interval '3 months - 1 day')::date
),

-- Combine all metrics
fund_metrics as (
    select
        f.fund_id,
        f.fund_name,
        f.fund_type,
        fs.period_end_date,
        -- Calculate fund NAV: prefer nav_amount_converted when available, fall back to calculated NAV
        -- Set to NULL for credit funds
        case
            when f.fund_type = 'CREDIT' then null
            else coalesce(
                fs.nav_amount_converted,
                coalesce(iv.total_instrument_fair_value, 0) + coalesce(fs.cash_amount, 0)
            )
        end as fund_nav,
        fs.total_commitments,
        fs.total_called_capital,
        -- Calculate unfunded commitment
        fs.total_commitments - fs.total_called_capital as unfunded_commitment,
        fs.total_distributions,
        -- Calculate PE metrics from actual values for equity funds, NULL for credit funds
        case 
            when f.fund_type = 'CREDIT' then null 
            else fs.total_distributions / nullif(fs.total_called_capital, 0)
        end as dpi,
        case 
            when f.fund_type = 'CREDIT' then null 
            else coalesce(
                fs.nav_amount_converted,
                coalesce(iv.total_instrument_fair_value, 0) + coalesce(fs.cash_amount, 0)
            ) / nullif(fs.total_called_capital, 0)
        end as rvpi,
        -- Calculate TVPI as (NAV + Distributions) / Called Capital, NULL for credit funds
        case 
            when f.fund_type = 'CREDIT' then null 
            else (
                coalesce(
                    fs.nav_amount_converted,
                    coalesce(iv.total_instrument_fair_value, 0) + coalesce(fs.cash_amount, 0)
                ) + fs.total_distributions
            ) / nullif(fs.total_called_capital, 0)
        end as tvpi,
        fs.expected_coc,
        coalesce(pcc.number_of_portfolio_companies, 0) as number_of_portfolio_companies,
        coalesce(pc.number_of_positions, 0) as number_of_positions,
        coalesce(ce.lines_of_credit_outstanding, 0) as lines_of_credit_outstanding,
        -- Credit-specific metrics (NULL for equity funds)
        case when f.fund_type = 'EQUITY' then null else ce.total_credit_exposure end as total_exposure,
        case when f.fund_type = 'EQUITY' then null else ce.total_principal_outstanding end as principal_outstanding,
        case when f.fund_type = 'EQUITY' then null else ce.total_undrawn_commitment end as undrawn_commitment,
        -- Use credit interest income for credit funds, fall back to fund snapshot interest_income
        case 
            when f.fund_type = 'CREDIT' then coalesce(cii.total_interest_income, 0)
            else fs.interest_income
        end as interest_income,
        coalesce(pnf.period_net_flows, 0) as period_net_flows,
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
    left join credit_interest_income cii
        on f.fund_id = cii.fund_id
        and fs.period_end_date = cii.period_end_date
    left join period_net_flows pnf
        on f.fund_id = pnf.fund_id
        and fs.period_end_date = pnf.period_end_date
),

-- Calculate peak outstanding credit using window function
final_metrics as (
    select
        fund_id,
        fund_name,
        fund_type,
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
        total_exposure,
        principal_outstanding,
        undrawn_commitment,
        interest_income,
        period_net_flows,
        as_of_date
    from fund_metrics
)

select * from final_metrics
