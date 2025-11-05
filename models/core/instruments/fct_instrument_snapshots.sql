{{
    config(
        materialized='table',
        tags=['marts', 'core', 'fact']
    )
}}

-- Fact table for instrument valuations - common snapshot fields only
-- Type-specific metrics moved to fct_instrument_snapshots_equity and fct_instrument_snapshots_credit
with instrument_snapshots as (
    select
        instrument_snapshot_id,
        instrument_id,
        period_start_date,
        period_end_date,
        frequency,
        reporting_basis,
        snapshot_source,
        currency_code,
        fx_rate,
        fx_rate_as_of,
        fx_rate_source,
        fair_value,
        -- Calculate FX conversions for common fields
        case
            when fx_rate is not null and fair_value is not null
            then fair_value * fx_rate
            else null
        end as fair_value_converted,
        accrued_income,
        case
            when fx_rate is not null and accrued_income is not null
            then accrued_income * fx_rate
            else null
        end as accrued_income_converted,
        snapshot_source_file_ref,
        created_at,
        updated_at
    from {{ ref('int_instrument_snapshots_curated') }}
)

select * from instrument_snapshots
