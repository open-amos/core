{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimension']
    )
}}

with commitments as (
    select
        commitment_id,
        fund_id,
        investor_id,
        currency_code,
        original_amount,
        agreement_date,
        created_at,
        updated_at
    from {{ ref('int_commitments_curated') }}
)

select * from commitments
