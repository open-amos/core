-- Fact table for investor commitment snapshots
-- Thin mart layer selecting from int_commitment_snapshots_curated
with commitment_snapshots as (
    select
        commitment_snapshot_id,
        commitment_id,
        period_start_date,
        period_end_date,
        frequency,
        reporting_basis,
        snapshot_source,
        currency_code,
        committed_capital,
        called_capital,
        total_distributions,
        source,
        source_reference,
        created_at,
        updated_at
    from {{ ref('int_commitment_snapshots_curated') }}
)

select * from commitment_snapshots
