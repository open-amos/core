-- Fact table for company valuations
-- Thin mart layer selecting from int_company_valuations_curated
with company_valuations as (
    select
        company_valuation_id,
        company_id,
        period_start_date,
        period_end_date,
        frequency,
        reporting_basis,
        snapshot_source,
        amount,
        valuation_type,
        created_at,
        updated_at
    from {{ ref('int_company_valuations_curated') }}
)

select * from company_valuations
