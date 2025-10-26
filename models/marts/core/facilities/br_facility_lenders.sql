{{
    config(
        materialized='table',
        tags=['marts', 'core', 'bridge']
    )
}}

with facility_lenders as (
    select
        facility_id,
        counterparty_id,
        fund_id,
        syndicate_role,
        commitment_amount,
        allocation_pct,
        primary_flag,
        created_at,
        updated_at
    from {{ ref('int_facility_lenders_curated') }}
)

select * from facility_lenders
