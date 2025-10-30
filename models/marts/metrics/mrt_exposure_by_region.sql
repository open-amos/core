{{
    config(
        materialized='table',
        tags=['marts', 'metrics', 'bi']
    )
}}

-- Time-series exposure by region showing cumulative growth as opportunities close
with latest_fx_rates as (
    -- Get the most recent FX rate for each currency pair to USD
    select
        quote_currency,
        exchange_rate,
        row_number() over (
            partition by quote_currency 
            order by rate_date desc
        ) as rn
    from {{ ref('stg_ref__fx_rates') }}
    where base_currency = 'USD'
),

latest_fx_rates_filtered as (
    select
        quote_currency,
        exchange_rate
    from latest_fx_rates
    where rn = 1
),

latest_instrument_snapshots as (
    -- Get the most recent snapshot per instrument
    select
        snap.instrument_id,
        snap.period_end_date,
        snap.fair_value,
        snap.currency_code,
        row_number() over (
            partition by snap.instrument_id 
            order by snap.period_end_date desc
        ) as rn
    from {{ ref('fct_instrument_snapshots') }} snap
    inner join {{ ref('dim_instruments') }} inst
        on snap.instrument_id = inst.instrument_id
),

deployed_capital_by_instrument as (
    -- Convert deployed capital to USD
    select
        lis.instrument_id,
        lis.period_end_date,
        case
            when lis.currency_code = 'USD' then coalesce(lis.fair_value, 0)
            else coalesce(lis.fair_value, 0) / coalesce(fx.exchange_rate, 1.0)
        end as fair_value_usd
    from latest_instrument_snapshots lis
    left join latest_fx_rates_filtered fx
        on lis.currency_code = fx.quote_currency
    where lis.rn = 1
        and lis.fair_value is not null
),

deployed_by_region as (
    -- Deployed capital by region (current baseline)
    select
        coalesce(c.region, 'Unknown Region') as region,
        sum(
            dci.fair_value_usd * 
            case
                when bc.allocation_pct is not null then bc.allocation_pct / 100.0
                when bc.country_code is not null then 1.0
                else 1.0
            end
        ) as deployed_capital_usd,
        max(dci.period_end_date) as as_of_date
    from deployed_capital_by_instrument dci
    left join {{ ref('br_instrument_countries') }} bc
        on dci.instrument_id = bc.instrument_id
        and (bc.valid_from is null or dci.period_end_date >= bc.valid_from)
        and (bc.valid_to is null or dci.period_end_date <= bc.valid_to)
    left join {{ ref('dim_countries') }} c
        on bc.country_code = c.country_iso2_code
    group by coalesce(c.region, 'Unknown Region')
),

active_opportunities as (
    -- Get active opportunities with close dates
    select
        opp.opportunity_id,
        opp.amount,
        opp.close_date,
        opp.stage_id
    from {{ ref('dim_opportunities') }} opp
    inner join {{ ref('dim_stages') }} stg
        on opp.stage_id = stg.stage_id
    where stg.name not in ('Declined', 'Committed')
        and opp.close_date is not null
),

pipeline_by_region_month as (
    -- Pipeline opportunities by region and close month
    select
        date_trunc('month', ao.close_date) as close_month,
        coalesce(c.region, 'Unknown Region') as region,
        sum(
            ao.amount * coalesce(boc.allocation_pct / 100.0, 1.0)
        ) as pipeline_value_usd
    from active_opportunities ao
    left join {{ ref('br_opportunity_countries') }} boc
        on ao.opportunity_id = boc.opportunity_id
    left join {{ ref('dim_countries') }} c
        on boc.country_code = c.country_iso2_code
    group by date_trunc('month', ao.close_date), coalesce(c.region, 'Unknown Region')
),

-- Generate month spine: current month (October 2025) plus next 12 months
month_spine as (
    select
        date_trunc('month', current_date) + (seq.month_offset || ' months')::interval as exposure_month
    from (
        select 0 as month_offset union all
        select 1 union all select 2 union all select 3 union all
        select 4 union all select 5 union all select 6 union all
        select 7 union all select 8 union all select 9 union all
        select 10 union all select 11 union all select 12
    ) seq
),

region_spine as (
    select distinct region from deployed_by_region
    union
    select distinct region from pipeline_by_region_month
),

-- Cross join to get all region-month combinations
region_month_spine as (
    select
        ms.exposure_month,
        rs.region
    from month_spine ms
    cross join region_spine rs
),

-- Calculate cumulative closed opportunities up to and including each month
cumulative_closed_pipeline as (
    select
        rms.exposure_month,
        rms.region,
        coalesce(sum(prm.pipeline_value_usd), 0) as closed_pipeline_usd
    from region_month_spine rms
    left join pipeline_by_region_month prm
        on rms.region = prm.region
        and prm.close_month <= rms.exposure_month
    group by rms.exposure_month, rms.region
),

final as (
    -- Combine deployed capital with cumulative closed pipeline
    select
        ccp.exposure_month,
        extract(year from ccp.exposure_month) as exposure_year,
        extract(month from ccp.exposure_month) as exposure_month_num,
        ccp.region,
        coalesce(dr.deployed_capital_usd, 0) as deployed_capital_usd,
        ccp.closed_pipeline_usd,
        coalesce(dr.deployed_capital_usd, 0) + ccp.closed_pipeline_usd as total_exposure_usd,
        case
            when ccp.exposure_month = date_trunc('month', current_date) then 'Current'
            when ccp.exposure_month < date_trunc('month', current_date) then 'Historical'
            else 'Forecast'
        end as period_type,
        dr.as_of_date
    from cumulative_closed_pipeline ccp
    left join deployed_by_region dr
        on ccp.region = dr.region
)

select * from final
order by region, exposure_month
