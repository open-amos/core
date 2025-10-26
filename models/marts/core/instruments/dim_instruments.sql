{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimension']
    )
}}

with instruments as (
    select
        instrument_id,
        fund_id,
        company_id,
        instrument_type,
        currency_code,
        inception_date,
        termination_date,
        description,
        created_at,
        updated_at
    from {{ ref('int_instruments_unified') }}
)

select * from instruments
