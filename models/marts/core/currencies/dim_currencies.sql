{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimension']
    )
}}

with currencies as (
    select
        currency_code,
        currency_name as name,
        currency_symbol as symbol,
        created_date as created_at,
        last_modified_date as updated_at
    from {{ ref('ref_currencies') }}
)

select * from currencies
