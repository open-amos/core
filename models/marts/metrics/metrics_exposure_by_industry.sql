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

-- Base: deployed capital by industry and fund
deployed_base as (
    select
        coalesce(cast(bii.industry_id as text), 'UNKNOWN') as industry_id,
        coalesce(di.name, 'Unknown Industry') as industry_name,
        inst.fund_id,
        f.name as fund_name,
        sum(dci.fair_value_usd * coalesce(bii.allocation_pct / 100.0, 1.0)) as deployed_capital_usd,
        max(dci.period_end_date) as as_of_date
    from deployed_capital_by_instrument dci
    inner join {{ ref('dim_instruments') }} inst on dci.instrument_id = inst.instrument_id
    inner join {{ ref('dim_funds') }} f on inst.fund_id = f.fund_id
    left join {{ ref('br_instrument_industries') }} bii on dci.instrument_id = bii.instrument_id
        and (bii.valid_from is null or dci.period_end_date >= bii.valid_from)
        and (bii.valid_to is null or dci.period_end_date <= bii.valid_to)
    left join {{ ref('dim_industries') }} di on bii.industry_id = di.industry_id
    group by coalesce(cast(bii.industry_id as text), 'UNKNOWN'), coalesce(di.name, 'Unknown Industry'), inst.fund_id, f.name
),

-- Aggregations for deployed capital
deployed_all_industries as (
    select 'ALL' as industry_id, 'All Industries' as industry_name,
           fund_id, fund_name,
           sum(deployed_capital_usd) as deployed_capital_usd,
           max(as_of_date) as as_of_date
    from deployed_base group by fund_id, fund_name
),

deployed_all_funds as (
    select industry_id, industry_name,
           'ALL' as fund_id, 'All Funds' as fund_name,
           sum(deployed_capital_usd) as deployed_capital_usd,
           max(as_of_date) as as_of_date
    from deployed_base group by industry_id, industry_name
),

deployed_all_industries_all_funds as (
    select 'ALL' as industry_id, 'All Industries' as industry_name,
           'ALL' as fund_id, 'All Funds' as fund_name,
           sum(deployed_capital_usd) as deployed_capital_usd,
           max(as_of_date) as as_of_date
    from deployed_base
),

deployed_combined as (
    select * from deployed_base
    union all select * from deployed_all_industries
    union all select * from deployed_all_funds
    union all select * from deployed_all_industries_all_funds
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

-- Base: pipeline by industry, fund, and stage (cumulative from stage onwards)
pipeline_base as (
    select
        date_trunc('month', ao.close_date) as close_month,
        coalesce(cast(boi.industry_id as text), 'UNKNOWN') as industry_id,
        coalesce(di.name, 'Unknown Industry') as industry_name,
        ao.fund_id,
        f.name as fund_name,
        s.stage_id,
        s.stage_name,
        sum(ao.amount) as pipeline_value_usd
    from active_opportunities ao
    inner join {{ ref('dim_funds') }} f on ao.fund_id = f.fund_id
    left join {{ ref('br_opportunity_industries') }} boi on ao.opportunity_id = boi.opportunity_id
    left join {{ ref('dim_industries') }} di on boi.industry_id = di.industry_id
    cross join all_stages s
    where ao.stage_order >= s.stage_order
      and coalesce(boi.primary_flag, true)
    group by date_trunc('month', ao.close_date), coalesce(cast(boi.industry_id as text), 'UNKNOWN'),
             coalesce(di.name, 'Unknown Industry'), ao.fund_id, f.name, s.stage_id, s.stage_name
),

-- Aggregations for pipeline
pipeline_all_industries as (
    select close_month, 'ALL' as industry_id, 'All Industries' as industry_name,
           fund_id, fund_name, stage_id, stage_name, sum(pipeline_value_usd) as pipeline_value_usd
    from pipeline_base group by close_month, fund_id, fund_name, stage_id, stage_name
),

pipeline_all_funds as (
    select close_month, industry_id, industry_name,
           'ALL' as fund_id, 'All Funds' as fund_name,
           stage_id, stage_name, sum(pipeline_value_usd) as pipeline_value_usd
    from pipeline_base group by close_month, industry_id, industry_name, stage_id, stage_name
),

pipeline_all_stages as (
    select close_month, industry_id, industry_name,
           fund_id, fund_name,
           'ALL' as stage_id, 'All Stages' as stage_name, sum(pipeline_value_usd) as pipeline_value_usd
    from pipeline_base group by close_month, industry_id, industry_name, fund_id, fund_name
),

pipeline_all_industries_all_funds as (
    select close_month, 'ALL' as industry_id, 'All Industries' as industry_name,
           'ALL' as fund_id, 'All Funds' as fund_name, stage_id, stage_name,
           sum(pipeline_value_usd) as pipeline_value_usd
    from pipeline_base group by close_month, stage_id, stage_name
),

pipeline_all_industries_all_stages as (
    select close_month, 'ALL' as industry_id, 'All Industries' as industry_name,
           fund_id, fund_name, 'ALL' as stage_id, 'All Stages' as stage_name,
           sum(pipeline_value_usd) as pipeline_value_usd
    from pipeline_base group by close_month, fund_id, fund_name
),

pipeline_all_funds_all_stages as (
    select close_month, industry_id, industry_name,
           'ALL' as fund_id, 'All Funds' as fund_name, 'ALL' as stage_id, 'All Stages' as stage_name,
           sum(pipeline_value_usd) as pipeline_value_usd
    from pipeline_base group by close_month, industry_id, industry_name
),

pipeline_all_industries_all_funds_all_stages as (
    select close_month, 'ALL' as industry_id, 'All Industries' as industry_name,
           'ALL' as fund_id, 'All Funds' as fund_name, 'ALL' as stage_id, 'All Stages' as stage_name,
           sum(pipeline_value_usd) as pipeline_value_usd
    from pipeline_base group by close_month
),

pipeline_combined as (
    select * from pipeline_base
    union all select * from pipeline_all_industries
    union all select * from pipeline_all_funds
    union all select * from pipeline_all_stages
    union all select * from pipeline_all_industries_all_funds
    union all select * from pipeline_all_industries_all_stages
    union all select * from pipeline_all_funds_all_stages
    union all select * from pipeline_all_industries_all_funds_all_stages
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
    select distinct industry_id, industry_name, fund_id, fund_name, stage_id, stage_name
    from (
        select industry_id, industry_name, fund_id, fund_name, 'ALL' as stage_id, 'All Stages' as stage_name
        from deployed_combined
        union
        select industry_id, industry_name, fund_id, fund_name, stage_id, stage_name
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
        sp.exposure_month, sp.industry_id, sp.industry_name,
        sp.fund_id, sp.fund_name, sp.stage_id, sp.stage_name,
        coalesce(sum(pc.pipeline_value_usd), 0) as closed_pipeline_usd
    from full_spine sp
    left join pipeline_combined pc
        on sp.industry_id = pc.industry_id
        and sp.fund_id = pc.fund_id
        and sp.stage_id = pc.stage_id
        and pc.close_month <= sp.exposure_month
    group by sp.exposure_month, sp.industry_id, sp.industry_name,
             sp.fund_id, sp.fund_name, sp.stage_id, sp.stage_name
),

final as (
    select
        ccp.exposure_month,
        extract(year from ccp.exposure_month) as exposure_year,
        extract(month from ccp.exposure_month) as exposure_month_num,
        ccp.industry_id,
        ccp.industry_name,
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
        on ccp.industry_id = dc.industry_id
        and ccp.fund_id = dc.fund_id
)

select * from final
order by industry_name, fund_name, stage_name, exposure_month

