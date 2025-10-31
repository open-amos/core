{{
    config(
        materialized='table',
        tags=['marts', 'metrics', 'bi']
    )
}}

-- Time-series exposure with country, region, fund, and stage breakdowns
-- Grain: one row per country per region per month per fund per stage
with latest_fx_rates as (
    select
        quote_currency,
        exchange_rate,
        row_number() over (partition by quote_currency order by rate_date desc) as rn
    from {{ ref('stg_ref__fx_rates') }}
    where base_currency = 'USD'
),

latest_fx_rates_filtered as (
    select quote_currency, exchange_rate
    from latest_fx_rates
    where rn = 1
),

latest_instrument_snapshots as (
    select
        snap.instrument_id,
        snap.period_end_date,
        snap.fair_value,
        snap.currency_code,
        row_number() over (partition by snap.instrument_id order by snap.period_end_date desc) as rn
    from {{ ref('fct_instrument_snapshots') }} snap
    inner join {{ ref('dim_instruments') }} inst on snap.instrument_id = inst.instrument_id
),

deployed_capital_by_instrument as (
    select
        lis.instrument_id,
        lis.period_end_date,
        case
            when lis.currency_code = 'USD' then coalesce(lis.fair_value, 0)
            else coalesce(lis.fair_value, 0) / coalesce(fx.exchange_rate, 1.0)
        end as fair_value_usd
    from latest_instrument_snapshots lis
    left join latest_fx_rates_filtered fx on lis.currency_code = fx.quote_currency
    where lis.rn = 1 and lis.fair_value is not null
),

-- Base: deployed capital by country and fund
deployed_base as (
    select
        coalesce(c.region, 'Unknown Region') as region,
        coalesce(bc.country_code, 'UNKNOWN') as country_code,
        coalesce(c.name, 'Unknown Country') as country_name,
        inst.fund_id,
        f.name as fund_name,
        sum(dci.fair_value_usd * case
            when bc.allocation_pct is not null then bc.allocation_pct / 100.0
            when bc.country_code is not null then 1.0
            else 1.0
        end) as deployed_capital_usd,
        max(dci.period_end_date) as as_of_date
    from deployed_capital_by_instrument dci
    inner join {{ ref('dim_instruments') }} inst on dci.instrument_id = inst.instrument_id
    inner join {{ ref('dim_funds') }} f on inst.fund_id = f.fund_id
    left join {{ ref('br_instrument_countries') }} bc on dci.instrument_id = bc.instrument_id
        and (bc.valid_from is null or dci.period_end_date >= bc.valid_from)
        and (bc.valid_to is null or dci.period_end_date <= bc.valid_to)
    left join {{ ref('dim_countries') }} c on bc.country_code = c.country_iso2_code
    group by coalesce(c.region, 'Unknown Region'), coalesce(bc.country_code, 'UNKNOWN'),
             coalesce(c.name, 'Unknown Country'), inst.fund_id, f.name
),

-- Aggregations for deployed capital
deployed_all_countries as (
    select region, 'ALL' as country_code, 'All Countries' as country_name, fund_id, fund_name,
           sum(deployed_capital_usd) as deployed_capital_usd, max(as_of_date) as as_of_date
    from deployed_base group by region, fund_id, fund_name
),

deployed_all_funds as (
    select region, country_code, country_name, 'ALL' as fund_id, 'All Funds' as fund_name,
           sum(deployed_capital_usd) as deployed_capital_usd, max(as_of_date) as as_of_date
    from deployed_base group by region, country_code, country_name
),

deployed_all_countries_all_funds as (
    select region, 'ALL' as country_code, 'All Countries' as country_name, 'ALL' as fund_id, 'All Funds' as fund_name,
           sum(deployed_capital_usd) as deployed_capital_usd, max(as_of_date) as as_of_date
    from deployed_base group by region
),

deployed_all_regions as (
    select 'All Regions' as region, country_code, country_name, fund_id, fund_name,
           sum(deployed_capital_usd) as deployed_capital_usd, max(as_of_date) as as_of_date
    from deployed_base group by country_code, country_name, fund_id, fund_name
),

deployed_all_regions_all_countries as (
    select 'All Regions' as region, 'ALL' as country_code, 'All Countries' as country_name, fund_id, fund_name,
           sum(deployed_capital_usd) as deployed_capital_usd, max(as_of_date) as as_of_date
    from deployed_base group by fund_id, fund_name
),

deployed_all_regions_all_funds as (
    select 'All Regions' as region, country_code, country_name, 'ALL' as fund_id, 'All Funds' as fund_name,
           sum(deployed_capital_usd) as deployed_capital_usd, max(as_of_date) as as_of_date
    from deployed_base group by country_code, country_name
),

deployed_all_regions_all_countries_all_funds as (
    select 'All Regions' as region, 'ALL' as country_code, 'All Countries' as country_name,
           'ALL' as fund_id, 'All Funds' as fund_name,
           sum(deployed_capital_usd) as deployed_capital_usd, max(as_of_date) as as_of_date
    from deployed_base
),

deployed_combined as (
    select * from deployed_base
    union all select * from deployed_all_countries
    union all select * from deployed_all_funds
    union all select * from deployed_all_countries_all_funds
    union all select * from deployed_all_regions
    union all select * from deployed_all_regions_all_countries
    union all select * from deployed_all_regions_all_funds
    union all select * from deployed_all_regions_all_countries_all_funds
),

-- Pipeline opportunities
active_opportunities as (
    select opp.opportunity_id, opp.fund_id, opp.amount, opp.close_date, opp.stage_id,
           stg.name as stage_name, stg."order" as stage_order
    from {{ ref('dim_opportunities') }} opp
    inner join {{ ref('dim_stages') }} stg on opp.stage_id = stg.stage_id
    where stg.name not in ('Declined', 'Committed') and opp.close_date is not null
),

all_stages as (
    select stage_id, name as stage_name, "order" as stage_order
    from {{ ref('dim_stages') }}
    where name not in ('Declined', 'Committed')
),

-- Base: pipeline by country, fund, and stage (cumulative from stage onwards)
pipeline_base as (
    select
        date_trunc('month', ao.close_date) as close_month,
        coalesce(c.region, 'Unknown Region') as region,
        coalesce(boc.country_code, 'UNKNOWN') as country_code,
        coalesce(c.name, 'Unknown Country') as country_name,
        ao.fund_id,
        f.name as fund_name,
        s.stage_id,
        s.stage_name,
        sum(ao.amount * coalesce(boc.allocation_pct / 100.0, 1.0)) as pipeline_value_usd
    from active_opportunities ao
    inner join {{ ref('dim_funds') }} f on ao.fund_id = f.fund_id
    left join {{ ref('br_opportunity_countries') }} boc on ao.opportunity_id = boc.opportunity_id
    left join {{ ref('dim_countries') }} c on boc.country_code = c.country_iso2_code
    cross join all_stages s
    where ao.stage_order >= s.stage_order
    group by date_trunc('month', ao.close_date), coalesce(c.region, 'Unknown Region'),
             coalesce(boc.country_code, 'UNKNOWN'), coalesce(c.name, 'Unknown Country'),
             ao.fund_id, f.name, s.stage_id, s.stage_name
),

-- Aggregations for pipeline
pipeline_all_countries as (
    select close_month, region, 'ALL' as country_code, 'All Countries' as country_name,
           fund_id, fund_name, stage_id, stage_name, sum(pipeline_value_usd) as pipeline_value_usd
    from pipeline_base group by close_month, region, fund_id, fund_name, stage_id, stage_name
),

pipeline_all_funds as (
    select close_month, region, country_code, country_name, 'ALL' as fund_id, 'All Funds' as fund_name,
           stage_id, stage_name, sum(pipeline_value_usd) as pipeline_value_usd
    from pipeline_base group by close_month, region, country_code, country_name, stage_id, stage_name
),

pipeline_all_stages as (
    select close_month, region, country_code, country_name, fund_id, fund_name,
           'ALL' as stage_id, 'All Stages' as stage_name, sum(pipeline_value_usd) as pipeline_value_usd
    from pipeline_base group by close_month, region, country_code, country_name, fund_id, fund_name
),

pipeline_all_countries_all_funds as (
    select close_month, region, 'ALL' as country_code, 'All Countries' as country_name,
           'ALL' as fund_id, 'All Funds' as fund_name, stage_id, stage_name,
           sum(pipeline_value_usd) as pipeline_value_usd
    from pipeline_base group by close_month, region, stage_id, stage_name
),

pipeline_all_countries_all_stages as (
    select close_month, region, 'ALL' as country_code, 'All Countries' as country_name,
           fund_id, fund_name, 'ALL' as stage_id, 'All Stages' as stage_name,
           sum(pipeline_value_usd) as pipeline_value_usd
    from pipeline_base group by close_month, region, fund_id, fund_name
),

pipeline_all_funds_all_stages as (
    select close_month, region, country_code, country_name, 'ALL' as fund_id, 'All Funds' as fund_name,
           'ALL' as stage_id, 'All Stages' as stage_name, sum(pipeline_value_usd) as pipeline_value_usd
    from pipeline_base group by close_month, region, country_code, country_name
),

pipeline_all_countries_all_funds_all_stages as (
    select close_month, region, 'ALL' as country_code, 'All Countries' as country_name,
           'ALL' as fund_id, 'All Funds' as fund_name, 'ALL' as stage_id, 'All Stages' as stage_name,
           sum(pipeline_value_usd) as pipeline_value_usd
    from pipeline_base group by close_month, region
),

pipeline_all_regions as (
    select close_month, 'All Regions' as region, country_code, country_name, fund_id, fund_name,
           stage_id, stage_name, sum(pipeline_value_usd) as pipeline_value_usd
    from pipeline_base group by close_month, country_code, country_name, fund_id, fund_name, stage_id, stage_name
),

pipeline_all_regions_all_countries as (
    select close_month, 'All Regions' as region, 'ALL' as country_code, 'All Countries' as country_name,
           fund_id, fund_name, stage_id, stage_name, sum(pipeline_value_usd) as pipeline_value_usd
    from pipeline_base group by close_month, fund_id, fund_name, stage_id, stage_name
),

pipeline_all_regions_all_funds as (
    select close_month, 'All Regions' as region, country_code, country_name, 'ALL' as fund_id, 'All Funds' as fund_name,
           stage_id, stage_name, sum(pipeline_value_usd) as pipeline_value_usd
    from pipeline_base group by close_month, country_code, country_name, stage_id, stage_name
),

pipeline_all_regions_all_stages as (
    select close_month, 'All Regions' as region, country_code, country_name, fund_id, fund_name,
           'ALL' as stage_id, 'All Stages' as stage_name, sum(pipeline_value_usd) as pipeline_value_usd
    from pipeline_base group by close_month, country_code, country_name, fund_id, fund_name
),

pipeline_all_regions_all_countries_all_funds as (
    select close_month, 'All Regions' as region, 'ALL' as country_code, 'All Countries' as country_name,
           'ALL' as fund_id, 'All Funds' as fund_name, stage_id, stage_name,
           sum(pipeline_value_usd) as pipeline_value_usd
    from pipeline_base group by close_month, stage_id, stage_name
),

pipeline_all_regions_all_countries_all_stages as (
    select close_month, 'All Regions' as region, 'ALL' as country_code, 'All Countries' as country_name,
           fund_id, fund_name, 'ALL' as stage_id, 'All Stages' as stage_name,
           sum(pipeline_value_usd) as pipeline_value_usd
    from pipeline_base group by close_month, fund_id, fund_name
),

pipeline_all_regions_all_funds_all_stages as (
    select close_month, 'All Regions' as region, country_code, country_name, 'ALL' as fund_id, 'All Funds' as fund_name,
           'ALL' as stage_id, 'All Stages' as stage_name, sum(pipeline_value_usd) as pipeline_value_usd
    from pipeline_base group by close_month, country_code, country_name
),

pipeline_all_regions_all_countries_all_funds_all_stages as (
    select close_month, 'All Regions' as region, 'ALL' as country_code, 'All Countries' as country_name,
           'ALL' as fund_id, 'All Funds' as fund_name, 'ALL' as stage_id, 'All Stages' as stage_name,
           sum(pipeline_value_usd) as pipeline_value_usd
    from pipeline_base group by close_month
),

pipeline_combined as (
    select * from pipeline_base
    union all select * from pipeline_all_countries
    union all select * from pipeline_all_funds
    union all select * from pipeline_all_stages
    union all select * from pipeline_all_countries_all_funds
    union all select * from pipeline_all_countries_all_stages
    union all select * from pipeline_all_funds_all_stages
    union all select * from pipeline_all_countries_all_funds_all_stages
    union all select * from pipeline_all_regions
    union all select * from pipeline_all_regions_all_countries
    union all select * from pipeline_all_regions_all_funds
    union all select * from pipeline_all_regions_all_stages
    union all select * from pipeline_all_regions_all_countries_all_funds
    union all select * from pipeline_all_regions_all_countries_all_stages
    union all select * from pipeline_all_regions_all_funds_all_stages
    union all select * from pipeline_all_regions_all_countries_all_funds_all_stages
),

-- Month spine
month_spine as (
    select date_trunc('month', current_date) + (seq.month_offset || ' months')::interval as exposure_month
    from (
        select 0 as month_offset union all select 1 union all select 2 union all select 3 union all
        select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all
        select 9 union all select 10 union all select 11 union all select 12
    ) seq
),

-- Spine of all combinations
dimension_combinations as (
    select distinct region, country_code, country_name, fund_id, fund_name, stage_id, stage_name
    from (
        select region, country_code, country_name, fund_id, fund_name, 'ALL' as stage_id, 'All Stages' as stage_name
        from deployed_combined
        union
        select region, country_code, country_name, fund_id, fund_name, stage_id, stage_name
        from pipeline_combined
    ) all_combos
),

full_spine as (
    select ms.exposure_month, dc.*
    from month_spine ms
    cross join dimension_combinations dc
),

-- Cumulative pipeline
cumulative_closed_pipeline as (
    select
        sp.exposure_month, sp.region, sp.country_code, sp.country_name,
        sp.fund_id, sp.fund_name, sp.stage_id, sp.stage_name,
        coalesce(sum(pc.pipeline_value_usd), 0) as closed_pipeline_usd
    from full_spine sp
    left join pipeline_combined pc
        on sp.region = pc.region
        and sp.country_code = pc.country_code
        and sp.fund_id = pc.fund_id
        and sp.stage_id = pc.stage_id
        and pc.close_month <= sp.exposure_month
    group by sp.exposure_month, sp.region, sp.country_code, sp.country_name,
             sp.fund_id, sp.fund_name, sp.stage_id, sp.stage_name
),

final as (
    select
        ccp.exposure_month,
        extract(year from ccp.exposure_month) as exposure_year,
        extract(month from ccp.exposure_month) as exposure_month_num,
        ccp.region,
        ccp.country_code,
        ccp.country_name,
        ccp.fund_id,
        ccp.fund_name,
        ccp.stage_id,
        ccp.stage_name,
        coalesce(dc.deployed_capital_usd, 0) as deployed_capital_usd,
        ccp.closed_pipeline_usd,
        coalesce(dc.deployed_capital_usd, 0) + ccp.closed_pipeline_usd as total_exposure_usd,
        case
            when ccp.exposure_month = date_trunc('month', current_date) then 'Current'
            when ccp.exposure_month < date_trunc('month', current_date) then 'Historical'
            else 'Forecast'
        end as period_type,
        dc.as_of_date
    from cumulative_closed_pipeline ccp
    left join deployed_combined dc
        on ccp.region = dc.region
        and ccp.country_code = dc.country_code
        and ccp.fund_id = dc.fund_id
)

select * from final
order by region, country_name, fund_name, stage_name, exposure_month
