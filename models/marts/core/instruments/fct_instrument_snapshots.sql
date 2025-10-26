-- Fact table for instrument valuations
-- Thin mart layer selecting from int_instrument_snapshots_curated
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
        fair_value,
        amortized_cost,
        principal_outstanding,
        undrawn_commitment,
        accrued_income,
        accrued_fees,
        fair_value_converted,
        amortized_cost_converted,
        principal_outstanding_converted,
        undrawn_commitment_converted,
        accrued_income_converted,
        accrued_fees_converted,
        equity_stake_pct,
        equity_dividends_cum,
        equity_exit_proceeds_actual,
        equity_exit_proceeds_forecast,
        snapshot_source_file_ref,
        created_at,
        updated_at
    from {{ ref('int_instrument_snapshots_curated') }}
)

select * from instrument_snapshots
