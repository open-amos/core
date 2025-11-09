{{
    config(
        materialized='table',
        unique_key=['instrument_id', 'period_end_date'],
        tags=['marts', 'metrics']
    )
}}

-- Position-level investment performance metrics
-- Grain: One row per instrument per period_end_date

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

funds as (
    select
        fund_id,
        name as fund_name
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
        period_end_date,
        fair_value_converted as current_fair_value,
        row_number() over (partition by instrument_id order by period_end_date desc) as rn
    from {{ ref('fct_instrument_snapshots') }}
),

current_valuations as (
    select
        instrument_id,
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

-- Calculate cumulative invested (contributions)
instrument_contributions as (
    select
        instrument_id,
        sum(amount_converted) as cumulative_invested
    from {{ ref('fct_instrument_cashflows') }}
    where instrument_cashflow_type in ('CONTRIBUTION', 'PURCHASE', 'DRAW')
    group by instrument_id
),

-- Calculate realized proceeds (distributions)
instrument_distributions as (
    select
        instrument_id,
        sum(amount_converted) as realized_proceeds
    from {{ ref('fct_instrument_cashflows') }}
    where instrument_cashflow_type in ('SALE', 'DISTRIBUTION', 'DIVIDEND', 'PRINCIPAL', 'PREPAYMENT')
    group by instrument_id
),

-- Combine all metrics
position_metrics as (
    select
        i.instrument_id,
        cv.period_end_date,
        f.fund_id,
        f.fund_name,
        i.company_id,
        c.company_name,
        i.instrument_type,
        i.inception_date as initial_investment_date,
        i.termination_date as exit_date,
        ie.initial_cost_converted as initial_cost,
        coalesce(ic.cumulative_invested, 0) as cumulative_invested,
        coalesce(id.realized_proceeds, 0) as realized_proceeds,
        coalesce(cv.current_fair_value, 0) as current_fair_value,
        -- Calculate total value
        coalesce(id.realized_proceeds, 0) + coalesce(cv.current_fair_value, 0) as total_value,
        -- Calculate gross MOIC with NULLIF protection
        (coalesce(id.realized_proceeds, 0) + coalesce(cv.current_fair_value, 0)) 
            / nullif(coalesce(ic.cumulative_invested, 0), 0) as gross_moic,
        -- Calculate holding period in years
        (coalesce(i.termination_date, current_date) - i.inception_date) / 365.25 as holding_period_years,
        ie.initial_ownership_pct as ownership_pct_initial,
        eo.ownership_pct_current
    from instruments i
    inner join funds f on i.fund_id = f.fund_id
    left join companies c on i.company_id = c.company_id
    left join instruments_equity ie on i.instrument_id = ie.instrument_id
    left join current_valuations cv on i.instrument_id = cv.instrument_id
    left join current_equity_ownership eo on i.instrument_id = eo.instrument_id
    left join instrument_contributions ic on i.instrument_id = ic.instrument_id
    left join instrument_distributions id on i.instrument_id = id.instrument_id
    where cv.period_end_date is not null  -- Only include instruments with snapshots
),

-- Calculate IRR using POWER function approximation
final_metrics as (
    select
        instrument_id,
        period_end_date,
        fund_id,
        fund_name,
        company_id,
        company_name,
        instrument_type,
        initial_investment_date,
        exit_date,
        initial_cost,
        cumulative_invested,
        realized_proceeds,
        current_fair_value,
        total_value,
        gross_moic,
        -- Calculate gross IRR using MOIC and holding period approximation
        case
            when gross_moic is not null 
                and holding_period_years > 0 
                and gross_moic > 0
            then power(gross_moic, 1.0 / nullif(holding_period_years, 0)) - 1
            else null
        end as gross_irr,
        ownership_pct_initial,
        ownership_pct_current,
        holding_period_years
    from position_metrics
)

select * from final_metrics
