{{
    config(
        materialized='table',
        tags=['marts', 'core', 'fact']
    )
}}

-- Fact table for credit-specific snapshot metrics
-- 1:1 relationship with fct_instrument_snapshots via instrument_snapshot_id
with credit_snapshots as (
    select
        s.instrument_snapshot_id,
        s.principal_outstanding,
        case
            when s.fx_rate is not null and s.principal_outstanding is not null
            then s.principal_outstanding * s.fx_rate
            else null
        end as principal_outstanding_converted,
        s.undrawn_commitment,
        case
            when s.fx_rate is not null and s.undrawn_commitment is not null
            then s.undrawn_commitment * s.fx_rate
            else null
        end as undrawn_commitment_converted,
        s.accrued_interest,
        case
            when s.fx_rate is not null and s.accrued_interest is not null
            then s.accrued_interest * s.fx_rate
            else null
        end as accrued_interest_converted,
        s.accrued_fees,
        case
            when s.fx_rate is not null and s.accrued_fees is not null
            then s.accrued_fees * s.fx_rate
            else null
        end as accrued_fees_converted,
        s.amortized_cost,
        case
            when s.fx_rate is not null and s.amortized_cost is not null
            then s.amortized_cost * s.fx_rate
            else null
        end as amortized_cost_converted,
        s.expected_loss,
        s.status,
        current_timestamp as created_at,
        current_timestamp as updated_at
    from {{ ref('int_instrument_snapshots_curated') }} s
    inner join {{ ref('dim_instruments') }} i
        on s.instrument_id = i.instrument_id
    where i.instrument_type = 'CREDIT'
        and s.principal_outstanding is not null
)

select * from credit_snapshots
