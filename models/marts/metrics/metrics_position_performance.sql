{{
    config(
        materialized='table',
        unique_key=['instrument_id', 'period_end_date'],
        tags=['marts', 'metrics']
    )
}}

-- Position-level investment performance metrics
-- Grain: One row per instrument per period_end_date
-- Supports both equity and credit instruments with strategy-specific metrics

with instruments as (
    select
        instrument_id,
        fund_id,
        company_id,
        name as instrument_name,
        instrument_type,
        inception_date,
        termination_date
    from {{ ref('dim_instruments') }}
),

instruments_equity as (
    select
        instrument_id,
        initial_ownership_pct,
        initial_cost_converted
    from {{ ref('dim_instruments_equity') }}
),

instruments_credit as (
    select
        instrument_id,
        commitment_amount_converted as commitment_amount,
        maturity_date,
        interest_index,
        spread_bps,
        security_rank
    from {{ ref('dim_instruments_credit') }}
),

funds as (
    select
        fund_id,
        name as fund_name,
        type as fund_type
    from {{ ref('dim_funds') }}
),

companies as (
    select
        company_id,
        name as company_name
    from {{ ref('dim_companies') }}
),

-- Get latest snapshot per instrument
latest_snapshots as (
    select
        instrument_id,
        instrument_snapshot_id,
        period_end_date,
        fair_value_converted as current_fair_value,
        row_number() over (partition by instrument_id order by period_end_date desc) as rn
    from {{ ref('fct_instrument_snapshots') }}
),

current_valuations as (
    select
        instrument_id,
        instrument_snapshot_id,
        period_end_date,
        current_fair_value
    from latest_snapshots
    where rn = 1
),

-- Get current equity ownership from latest equity snapshot
latest_equity_snapshots as (
    select
        s.instrument_id,
        se.equity_stake_pct as ownership_pct_current,
        row_number() over (partition by s.instrument_id order by s.period_end_date desc) as rn
    from {{ ref('fct_instrument_snapshots') }} s
    inner join {{ ref('fct_instrument_snapshots_equity') }} se
        on s.instrument_snapshot_id = se.instrument_snapshot_id
),

current_equity_ownership as (
    select
        instrument_id,
        ownership_pct_current
    from latest_equity_snapshots
    where rn = 1
),

-- Get current credit metrics from latest credit snapshot
latest_credit_snapshots as (
    select
        s.instrument_id,
        sc.principal_outstanding_converted,
        sc.undrawn_commitment_converted,
        sc.accrued_interest_converted,
        row_number() over (partition by s.instrument_id order by s.period_end_date desc) as rn
    from {{ ref('fct_instrument_snapshots') }} s
    inner join {{ ref('fct_instrument_snapshots_credit') }} sc
        on s.instrument_snapshot_id = sc.instrument_snapshot_id
),

current_credit_metrics as (
    select
        instrument_id,
        principal_outstanding_converted as principal_outstanding,
        undrawn_commitment_converted as undrawn_commitment,
        accrued_interest_converted as accrued_interest
    from latest_credit_snapshots
    where rn = 1
),

-- Calculate drawn amount for credit instruments (from cashflows)
credit_drawn_amount as (
    select
        instrument_id,
        sum(case when instrument_cashflow_type = 'DRAW' then amount_converted else 0 end) -
        sum(case when instrument_cashflow_type in ('PRINCIPAL', 'PREPAYMENT') then amount_converted else 0 end) as drawn_amount,
        sum(case when instrument_cashflow_type = 'INTEREST' then amount_converted else 0 end) as interest_income
    from {{ ref('fct_instrument_cashflows') }}
    group by instrument_id
),

-- Calculate realized proceeds for equity (distributions and exits)
equity_realized_proceeds as (
    select
        instrument_id,
        sum(amount_converted) as realized_proceeds
    from {{ ref('fct_instrument_cashflows') }}
    where instrument_cashflow_type in ('EXIT_PROCEEDS', 'RETURN_OF_CAPITAL', 'DIVIDEND')
    group by instrument_id
),

-- Calculate realized proceeds for credit (principal repayments)
credit_realized_proceeds as (
    select
        instrument_id,
        sum(amount_converted) as realized_proceeds
    from {{ ref('fct_instrument_cashflows') }}
    where instrument_cashflow_type in ('PRINCIPAL', 'PREPAYMENT')
    group by instrument_id
),

-- Combine all metrics
position_metrics as (
    select
        i.instrument_id,
        i.instrument_name,
        i.instrument_type,
        cv.period_end_date,
        f.fund_id,
        f.fund_name,
        f.fund_type,
        i.company_id,
        c.company_name,
        i.inception_date as initial_investment_date,
        i.termination_date as exit_date,
        
        -- Equity-specific metrics (NULL for credit instruments)
        case when i.instrument_type = 'EQUITY' then ie.initial_cost_converted else null end as cost_basis,
        case when i.instrument_type = 'EQUITY' then coalesce(erp.realized_proceeds, 0) else null end as realized_proceeds,
        case when i.instrument_type = 'EQUITY' then coalesce(cv.current_fair_value, 0) else null end as fair_value,
        case when i.instrument_type = 'EQUITY' then ie.initial_ownership_pct else null end as ownership_pct_initial,
        case when i.instrument_type = 'EQUITY' then eo.ownership_pct_current else null end as ownership_pct_current,
        
        -- Credit-specific metrics (NULL for equity instruments)
        case when i.instrument_type = 'CREDIT' then coalesce(cda.drawn_amount, 0) else null end as drawn_amount,
        case when i.instrument_type = 'CREDIT' then coalesce(cda.interest_income, 0) else null end as interest_income,
        case when i.instrument_type = 'CREDIT' then coalesce(crp.realized_proceeds, 0) else null end as principal_repaid,
        case when i.instrument_type = 'CREDIT' then ccm.principal_outstanding else null end as principal_outstanding,
        case when i.instrument_type = 'CREDIT' then ccm.undrawn_commitment else null end as undrawn_commitment,
        case when i.instrument_type = 'CREDIT' then ccm.accrued_interest else null end as accrued_interest,
        case when i.instrument_type = 'CREDIT' then icr.commitment_amount else null end as commitment_amount,
        case when i.instrument_type = 'CREDIT' then icr.spread_bps else null end as spread_bps,
        case when i.instrument_type = 'CREDIT' then icr.interest_index else null end as interest_index,
        case when i.instrument_type = 'CREDIT' then icr.maturity_date else null end as maturity_date,
        case when i.instrument_type = 'CREDIT' then icr.security_rank else null end as security_rank,
        
        -- Calculate holding period in years
        (coalesce(i.termination_date, current_date) - i.inception_date) / 365.25 as holding_period_years
        
    from instruments i
    inner join funds f on i.fund_id = f.fund_id
    left join companies c on i.company_id = c.company_id
    left join instruments_equity ie on i.instrument_id = ie.instrument_id
    left join instruments_credit icr on i.instrument_id = icr.instrument_id
    left join current_valuations cv on i.instrument_id = cv.instrument_id
    left join current_equity_ownership eo on i.instrument_id = eo.instrument_id
    left join current_credit_metrics ccm on i.instrument_id = ccm.instrument_id
    left join equity_realized_proceeds erp on i.instrument_id = erp.instrument_id
    left join credit_drawn_amount cda on i.instrument_id = cda.instrument_id
    left join credit_realized_proceeds crp on i.instrument_id = crp.instrument_id
    where cv.period_end_date is not null  -- Only include instruments with snapshots
),

-- Calculate derived metrics
final_metrics as (
    select
        instrument_id,
        instrument_name,
        instrument_type,
        period_end_date,
        fund_id,
        fund_name,
        fund_type,
        company_id,
        company_name,
        initial_investment_date,
        exit_date,
        holding_period_years,
        
        -- Equity metrics
        cost_basis,
        realized_proceeds,
        fair_value,
        ownership_pct_initial,
        ownership_pct_current,
        
        -- Calculate MOIC for equity instruments only (using cost_basis from dimension)
        case
            when instrument_type = 'EQUITY' and cost_basis > 0
            then (coalesce(realized_proceeds, 0) + coalesce(fair_value, 0)) / nullif(cost_basis, 0)
            else null
        end as moic,
        
        -- Calculate equity IRR approximation using MOIC and holding period
        -- IMPORTANT: This is an APPROXIMATION, not true XIRR from cashflows
        -- Formula: (MOIC ^ (1/holding_period_years)) - 1
        -- Limitations: Does not account for interim cashflows, follow-on investments, or partial exits
        -- Always label as "Approx IRR" or "IRR (Approx)" in visualizations
        case
            when instrument_type = 'EQUITY' 
                and cost_basis > 0
                and holding_period_years > 0
                and (coalesce(realized_proceeds, 0) + coalesce(fair_value, 0)) / nullif(cost_basis, 0) > 0
            then power(
                (coalesce(realized_proceeds, 0) + coalesce(fair_value, 0)) / nullif(cost_basis, 0),
                1.0 / nullif(holding_period_years, 0)
            ) - 1
            else null
        end as equity_irr_approx,
        
        -- Credit metrics
        drawn_amount,
        interest_income,
        principal_repaid,
        principal_outstanding,
        undrawn_commitment,
        accrued_interest,
        commitment_amount,
        spread_bps,
        interest_index,
        maturity_date,
        security_rank,
        
        -- Calculate utilization rate for credit instruments
        case
            when instrument_type = 'CREDIT' and commitment_amount > 0
            then principal_outstanding / nullif(commitment_amount, 0)
            else null
        end as utilization_rate,
        
        -- Calculate maturity_year for credit instruments
        case
            when instrument_type = 'CREDIT' and maturity_date is not null
            then extract(year from maturity_date)
            else null
        end as maturity_year,
        
        -- Calculate days_to_maturity for credit instruments
        case
            when instrument_type = 'CREDIT' and maturity_date is not null
            then maturity_date - current_date
            else null
        end as days_to_maturity,
        
        -- Calculate all_in_yield for credit instruments (spread + base rate)
        -- Note: Base rate would need to be joined from a reference table
        -- For now, we'll just use spread_bps converted to decimal
        case
            when instrument_type = 'CREDIT' and spread_bps is not null
            then spread_bps / 10000.0
            else null
        end as all_in_yield
        
    from position_metrics
)

select * from final_metrics
