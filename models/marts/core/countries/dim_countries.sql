{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimension']
    )
}}

with countries as (
    select
        iso2 as country_iso2_code,
        iso3 as country_iso3_code,
        name,
        region,
        sub_region as subregion,
        income_level,
        created_at,
        updated_at
    from {{ ref('ref_countries') }}
)

select * from countries
