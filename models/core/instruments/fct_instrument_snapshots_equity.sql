{{
    config(
        materialized='table',
        tags=['marts', 'core', 'fact']
    )
}}

-- Fact table for equity-specific snapshot metrics
-- 1:1 relationship with fct_instrument_snapshots via instrument_snapshot_id
with equity_snapshots as (
    select
        s.instrument_snapshot_id,
        s.equity_stake_pct,
        s.stake_basis,
        current_timestamp as created_at,
        current_timestamp as updated_at
    from {{ ref('int_instrument_snapshots_curated') }} s
    inner join {{ ref('dim_instruments') }} i
        on s.instrument_id = i.instrument_id
    where i.instrument_type = 'EQUITY'
        and s.equity_stake_pct is not null
)

select * from equity_snapshots
