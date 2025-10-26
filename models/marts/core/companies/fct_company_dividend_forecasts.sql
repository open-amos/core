-- Fact table for projected dividend payments
-- Thin mart layer selecting from int_company_dividend_forecasts_curated
with company_dividend_forecasts as (
    select
        company_dividend_forecast_id,
        company_id,
        date,
        amount,
        created_at,
        updated_at
    from {{ ref('int_company_dividend_forecasts_curated') }}
)

select * from company_dividend_forecasts
