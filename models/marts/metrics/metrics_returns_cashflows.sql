{{
    config(
        materialized='table',
        unique_key=['fund_id', 'instrument_id', 'cashflow_date', 'cashflow_type'],
        tags=['marts', 'metrics']
    )
}}

-- Standardized cashflow-level view for IRR and returns analysis
-- Grain: One row per cashflow transaction

with instrument_cashflows as (
    select
        instrument_cashflow_id,
        instrument_id,
        instrument_cashflow_type,
        date as cashflow_date,
        amount,
        currency_code,
        fx_rate,
        amount_converted
    from {{ ref('fct_instrument_cashflows') }}
),

instruments as (
    select
        instrument_id,
        fund_id,
        company_id,
        name as instrument_name
    from {{ ref('dim_instruments') }}
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

cashflows_enriched as (
    select
        f.fund_id,
        f.fund_name,
        i.instrument_id,
        i.instrument_name,
        i.company_id,
        c.company_name,
        cf.cashflow_date,
        cf.instrument_cashflow_type as cashflow_type,
        cf.amount_converted as cashflow_amount,
        cf.currency_code,
        cf.fx_rate,
        -- Classify direction based on cashflow type
        case 
            when cf.instrument_cashflow_type in ('CONTRIBUTION', 'PURCHASE', 'DRAW') 
            then 'outflow'
            else 'inflow'
        end as direction,
        -- Calculate signed amount for IRR calculations
        case 
            when cf.instrument_cashflow_type in ('CONTRIBUTION', 'PURCHASE', 'DRAW') 
            then -1 * cf.amount_converted
            else cf.amount_converted
        end as signed_amount
    from instrument_cashflows cf
    inner join instruments i on cf.instrument_id = i.instrument_id
    inner join funds f on i.fund_id = f.fund_id
    left join companies c on i.company_id = c.company_id
)

select * from cashflows_enriched
