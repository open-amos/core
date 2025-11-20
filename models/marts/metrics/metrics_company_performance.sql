{{
    config(
        materialized='table',
        unique_key=['company_id', 'period_end_date'],
        tags=['marts', 'metrics']
    )
}}

-- Company-level financial performance metrics
-- Grain: One row per company per period_end_date

with companies as (
    select
        company_id,
        name as company_name,
        website,
        description
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

-- Combine all metrics
company_metrics as (
    select
        c.company_id,
        c.company_name,
        c.website,
        c.description,
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
        -- Get primary country
        dc.name as country_name,
        -- Get primary industry
        di.name as industry_name,
        cf.reporting_currency
    from companies c
    inner join company_financials cf on c.company_id = cf.company_id
    left join latest_valuations lv 
        on c.company_id = lv.company_id 
        and cf.period_end_date = lv.period_end_date
    -- Join to primary country
    left join {{ ref('br_company_countries') }} bcc
        on c.company_id = bcc.company_id
        and bcc.primary_flag = true
    left join {{ ref('dim_countries') }} dc
        on bcc.country_code = dc.country_iso2_code
    -- Join to primary industry
    left join {{ ref('br_company_industries') }} bci
        on c.company_id = bci.company_id
        and bci.primary_flag = true
    left join {{ ref('dim_industries') }} di
        on bci.industry_id = di.industry_id
)

select * from company_metrics
