{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimension']
    )
}}

with counterparties as (
    select
        counterparty_id,
        name,
        type,
        country_code,
        created_at,
        updated_at
    from {{ ref('int_counterparties_unified') }}
)

select * from counterparties
