-- Fact table for commitment change history
-- Thin mart layer selecting from int_commitment_records_curated
with commitment_records as (
    select
        commitment_record_id,
        commitment_id,
        date_from,
        amount,
        status,
        created_at,
        updated_at
    from {{ ref('int_commitment_records_curated') }}
)

select * from commitment_records
