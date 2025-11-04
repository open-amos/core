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
        name,
        instrument_type,
        inception_date,
        termination_date,
        description,
        created_at,
        updated_at
    from {{ ref('int_instruments_unified') }}
)

select * from instruments
