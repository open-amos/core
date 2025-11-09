{{
    config(
        materialized='table',
        tags=['marts', 'metrics', 'bi']
    )
}}

with latest_instrument_snapshots as (
    select
        snap.instrument_id,
        snap.period_end_date,
        snap.fair_value,
        snap.fair_value_converted,
        snap.currency_code,
        snap.fx_rate,
        row_number() over (partition by snap.instrument_id order by snap.period_end_date desc) as rn
    from {{ ref('fct_instrument_snapshots') }} snap
    inner join {{ ref('dim_instruments') }} inst on snap.instrument_id = inst.instrument_id
),

deployed_capital_by_instrument as (
    select
        lis.instrument_id,
        lis.period_end_date,
        coalesce(lis.fair_value_converted,
                 case
                     when lis.fx_rate is not null then coalesce(lis.fair_value, 0) * lis.fx_rate
                     else coalesce(lis.fair_value, 0)
                 end) as fair_value_usd
    from latest_instrument_snapshots lis
    where lis.rn = 1 and lis.fair_value is not null
),

-- Base: deployed capital by all dimensions
deployed_base as (
    select
        inst.fund_id,
        f.name as fund_name,
        coalesce(c.region, 'Unknown Region') as region,
        coalesce(bc.country_code, 'UNKNOWN') as country_code,
        coalesce(c.name, 'Unknown Country') as country_name,
        coalesce(cast(bii.industry_id as text), 'UNKNOWN') as industry_id,
        coalesce(di.name, 'Unknown Industry') as industry_name,
        sum(dci.fair_value_usd 
            * coalesce(bii.allocation_pct / 100.0, 1.0)
            * case
                when bc.allocation_pct is not null then bc.allocation_pct / 100.0
                when bc.country_code is not null then 1.0
                else 1.0
            end) as deployed_capital_usd,
        max(dci.period_end_date) as as_of_date
    from deployed_capital_by_instrument dci
    inner join {{ ref('dim_instruments') }} inst on dci.instrument_id = inst.instrument_id
    inner join {{ ref('dim_funds') }} f on inst.fund_id = f.fund_id
    left join {{ ref('br_instrument_countries') }} bc on dci.instrument_id = bc.instrument_id
    left join {{ ref('dim_countries') }} c on bc.country_code = c.country_iso2_code
    left join {{ ref('br_instrument_industries') }} bii on dci.instrument_id = bii.instrument_id
        and (bii.valid_from is null or dci.period_end_date >= bii.valid_from)
        and (bii.valid_to is null or dci.period_end_date <= bii.valid_to)
    left join {{ ref('dim_industries') }} di on bii.industry_id = di.industry_id
    group by 
        inst.fund_id, f.name,
        coalesce(c.region, 'Unknown Region'),
        coalesce(bc.country_code, 'UNKNOWN'),
        coalesce(c.name, 'Unknown Country'),
        coalesce(cast(bii.industry_id as text), 'UNKNOWN'),
        coalesce(di.name, 'Unknown Industry')
),

-- Pipeline opportunities
active_opportunities as (
    select 
        opp.opportunity_id, 
        opp.fund_id, 
        opp.amount, 
        opp.close_date, 
        opp.stage_id,
        stg.name as stage_name, 
        stg."order" as stage_order
    from {{ ref('dim_opportunities') }} opp
    inner join {{ ref('dim_stages') }} stg on opp.stage_id = stg.stage_id
    where stg.name not in ('Declined', 'Committed') 
      and opp.close_date is not null
),

all_stages as (
    select stage_id, name as stage_name, "order" as stage_order
    from {{ ref('dim_stages') }}
    where name not in ('Declined', 'Committed')
),

-- Base: pipeline by all dimensions and stage (cumulative from stage onwards)
pipeline_base as (
    select
        date_trunc('month', ao.close_date) as close_month,
        ao.fund_id,
        f.name as fund_name,
        coalesce(c.region, 'Unknown Region') as region,
        coalesce(boc.country_code, 'UNKNOWN') as country_code,
        coalesce(c.name, 'Unknown Country') as country_name,
        coalesce(cast(boi.industry_id as text), 'UNKNOWN') as industry_id,
        coalesce(di.name, 'Unknown Industry') as industry_name,
        s.stage_id,
        s.stage_name,
        sum(ao.amount * coalesce(boc.allocation_pct / 100.0, 1.0)) as pipeline_value_usd
    from active_opportunities ao
    inner join {{ ref('dim_funds') }} f on ao.fund_id = f.fund_id
    left join {{ ref('br_opportunity_countries') }} boc on ao.opportunity_id = boc.opportunity_id
    left join {{ ref('dim_countries') }} c on boc.country_code = c.country_iso2_code
    left join {{ ref('br_opportunity_industries') }} boi on ao.opportunity_id = boi.opportunity_id
        and coalesce(boi.primary_flag, true)
    left join {{ ref('dim_industries') }} di on boi.industry_id = di.industry_id
    cross join all_stages s
    where ao.stage_order >= s.stage_order
    group by 
        date_trunc('month', ao.close_date),
        ao.fund_id, f.name,
        coalesce(c.region, 'Unknown Region'),
        coalesce(boc.country_code, 'UNKNOWN'),
        coalesce(c.name, 'Unknown Country'),
        coalesce(cast(boi.industry_id as text), 'UNKNOWN'),
        coalesce(di.name, 'Unknown Industry'),
        s.stage_id, s.stage_name
),

-- Month spine (13 months: current + 12 future)
month_spine as (
    select date_trunc('month', current_date) + (seq.month_offset || ' months')::interval as exposure_month
    from (
        select 0 as month_offset union all select 1 union all select 2 union all select 3 union all
        select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all
        select 9 union all select 10 union all select 11 union all select 12
    ) seq
),

-- All dimension combinations from deployed + pipeline
dimension_combinations as (
    select distinct 
        fund_id, fund_name, 
        region, country_code, country_name,
        industry_id, industry_name,
        stage_id, stage_name
    from (
        select 
            fund_id, fund_name,
            region, country_code, country_name,
            industry_id, industry_name,
            cast(null as text) as stage_id,
            cast(null as text) as stage_name
        from deployed_base
        union
        select 
            fund_id, fund_name,
            region, country_code, country_name,
            industry_id, industry_name,
            cast(stage_id as text) as stage_id,
            stage_name
        from pipeline_base
    ) all_combos
),

full_spine as (
    select 
        ms.exposure_month, 
        dc.fund_id, dc.fund_name,
        dc.region, dc.country_code, dc.country_name,
        dc.industry_id, dc.industry_name,
        dc.stage_id, dc.stage_name
    from month_spine ms
    cross join dimension_combinations dc
),

-- Cumulative pipeline
cumulative_closed_pipeline as (
    select
        sp.exposure_month,
        sp.fund_id, sp.fund_name,
        sp.region, sp.country_code, sp.country_name,
        sp.industry_id, sp.industry_name,
        sp.stage_id, sp.stage_name,
        coalesce(sum(pb.pipeline_value_usd), 0) as closed_pipeline_usd
    from full_spine sp
    left join pipeline_base pb
        on sp.fund_id = pb.fund_id
        and sp.region = pb.region
        and sp.country_code = pb.country_code
        and sp.industry_id = pb.industry_id
        and sp.stage_id = cast(pb.stage_id as text)
        and pb.close_month <= sp.exposure_month
    group by 
        sp.exposure_month,
        sp.fund_id, sp.fund_name,
        sp.region, sp.country_code, sp.country_name,
        sp.industry_id, sp.industry_name,
        sp.stage_id, sp.stage_name
),

final as (
    select
        ccp.exposure_month,
        extract(year from ccp.exposure_month) as exposure_year,
        extract(month from ccp.exposure_month) as exposure_month_num,
        ccp.fund_id,
        ccp.fund_name,
        ccp.region,
        ccp.country_code,
        ccp.country_name,
        ccp.industry_id,
        ccp.industry_name,
        ccp.stage_id,
        ccp.stage_name,
        coalesce(db.deployed_capital_usd, 0) as deployed_capital_usd,
        ccp.closed_pipeline_usd,
        coalesce(db.deployed_capital_usd, 0) + ccp.closed_pipeline_usd as total_exposure_usd,
        case
            when ccp.exposure_month = date_trunc('month', current_date) then 'Current'
            when ccp.exposure_month < date_trunc('month', current_date) then 'Historical'
            else 'Forecast'
        end as period_type,
        db.as_of_date
    from cumulative_closed_pipeline ccp
    left join deployed_base db
        on ccp.fund_id = db.fund_id
        and ccp.region = db.region
        and ccp.country_code = db.country_code
        and ccp.industry_id = db.industry_id
)

select * from final
order by fund_name, region, country_name, industry_name, stage_name, exposure_month
