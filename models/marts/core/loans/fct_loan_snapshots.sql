-- Fact table for loan position snapshots
-- Thin mart layer selecting from int_loan_snapshots_curated
with loan_snapshots as (
    select
        loan_snapshot_id,
        loan_id,
        period_start_date,
        period_end_date,
        frequency,
        reporting_basis,
        snapshot_source,
        principal_outstanding,
        undrawn_commitment,
        accrued_interest,
        accrued_fees,
        amortized_cost,
        fair_value,
        expected_loss,
        status,
        currency_code,
        fx_rate,
        principal_outstanding_converted,
        undrawn_commitment_converted,
        accrued_interest_converted,
        accrued_fees_converted,
        amortized_cost_converted,
        fair_value_converted,
        source_file_ref,
        created_at,
        updated_at
    from {{ ref('int_loan_snapshots_curated') }}
)

select * from loan_snapshots
