-- Test: NAV variance should be within ±2% for equity funds
-- Severity: WARN
-- This test identifies funds where the reported NAV from fund admin
-- differs from the calculated NAV (from instrument fair values) by more than 2%

{{ config(severity='warn') }}

select
    fund_id,
    fund_name,
    period_end_date,
    fund_nav_reported,
    fund_nav_calculated,
    nav_variance,
    nav_variance_pct
from {{ ref('metrics_fund_performance') }}
where abs(nav_variance_pct) > 0.02
    and fund_nav_reported is not null
    and fund_nav_calculated is not null
    and fund_type = 'EQUITY'
