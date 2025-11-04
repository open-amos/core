{{
    config(
        materialized='ephemeral',
        tags=['tests', 'helpers']
    )
}}

select
    instrument_id
from {{ ref('dim_instruments') }}
where instrument_type = 'LOAN'


