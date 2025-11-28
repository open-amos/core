-- Test: Distributions variance should be within ±0.1% for equity funds
-- Severity: ERROR
-- This test identifies funds where the reported distributions from fund admin
-- differs from the calculated distributions (from cashflows) by more than 0.1%

select
    fund_id,
    fund_name,
    period_end_date,
    total_distributions_reported,
    total_distributions_calculated,
    distributions_variance,
    distributions_variance_pct
from {{ ref('metrics_fund_performance') }}
where abs(distributions_variance_pct) > 0.001
    and total_distributions_reported is not null
    and total_distributions_calculated is not null
    and fund_type = 'EQUITY'
