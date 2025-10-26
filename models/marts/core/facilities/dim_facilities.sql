{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimension']
    )
}}

with facilities as (
    select
        facility_id,
        fund_id,
        borrower_company_id,
        facility_type,
        agent_counterparty_id,
        agreement_date,
        effective_date,
        maturity_date,
        currency_code,
        total_commitment,
        purpose,
        created_at,
        updated_at
    from {{ ref('int_facilities_curated') }}
)

select * from facilities
