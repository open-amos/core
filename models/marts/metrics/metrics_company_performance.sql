{{
    config(
        materialized='table',
        unique_key=['company_id', 'period_end_date'],
        tags=['marts', 'metrics', 'ilpa']
    )
}}

-- Company-level financial performance metrics aligned with ILPA reporting standards
-- Grain: One row per company per period_end_date

with companies as (
    select
        company_id,
        name as company_name
    from {{ ref('dim_companies') }}
),

company_financials as (
    select
        company_id,
        period_end_date,
        revenue,
        ebitda,
        cash,
        total_assets,
        total_liabilities,
        equity,
        currency_code as reporting_currency
    from {{ ref('fct_company_financials') }}
),

-- Get latest actual valuation for each company at each period
company_valuations as (
    select
        company_id,
        period_end_date,
        amount as enterprise_value,
        row_number() over (
            partition by company_id, period_end_date 
            order by period_end_date desc
        ) as rn
    from {{ ref('fct_company_valuations') }}
    where valuation_type = 'ACTUAL'
),

latest_valuations as (
    select
        company_id,
        period_end_date,
        enterprise_value
    from company_valuations
    where rn = 1
),

-- Get primary country with temporal validity check
primary_countries as (
    select
        bcc.company_id,
        c.name as primary_country,
        bcc.valid_from,
        bcc.valid_to
    from {{ ref('br_company_countries') }} bcc
    inner join {{ ref('dim_countries') }} c 
        on bcc.country_code = c.country_iso2_code
    where bcc.primary_flag = true
),

-- Get primary industry with temporal validity check
primary_industries as (
    select
        bci.company_id,
        i.name as primary_industry,
        bci.valid_from,
        bci.valid_to
    from {{ ref('br_company_industries') }} bci
    inner join {{ ref('dim_industries') }} i 
        on bci.industry_id = i.industry_id
    where bci.primary_flag = true
),

-- Combine all metrics
company_metrics as (
    select
        c.company_id,
        c.company_name,
        cf.period_end_date,
        cf.revenue,
        cf.ebitda,
        -- Calculate EBITDA margin with NULLIF protection
        (cf.ebitda / nullif(cf.revenue, 0)) * 100 as ebitda_margin,
        cf.cash,
        cf.total_assets,
        cf.total_liabilities,
        cf.equity,
        -- Calculate net debt
        cf.total_liabilities - cf.cash as net_debt,
        lv.enterprise_value,
        -- Calculate EV/EBITDA with NULLIF protection
        lv.enterprise_value / nullif(cf.ebitda, 0) as ev_to_ebitda,
        -- Calculate EV/Revenue with NULLIF protection
        lv.enterprise_value / nullif(cf.revenue, 0) as ev_to_revenue,
        -- Get primary country with temporal validity check
        (
            select pc.primary_country
            from primary_countries pc
            where pc.company_id = c.company_id
                and cf.period_end_date between pc.valid_from 
                    and coalesce(pc.valid_to, '9999-12-31')
            limit 1
        ) as primary_country,
        -- Get primary industry with temporal validity check
        (
            select pi.primary_industry
            from primary_industries pi
            where pi.company_id = c.company_id
                and cf.period_end_date between pi.valid_from 
                    and coalesce(pi.valid_to, '9999-12-31')
            limit 1
        ) as primary_industry,
        cf.reporting_currency
    from companies c
    inner join company_financials cf on c.company_id = cf.company_id
    left join latest_valuations lv 
        on c.company_id = lv.company_id 
        and cf.period_end_date = lv.period_end_date
)

select * from company_metrics
