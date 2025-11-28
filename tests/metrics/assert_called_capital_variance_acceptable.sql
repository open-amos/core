-- Test: Called capital variance should be within ±0.1% for equity funds
-- Severity: ERROR
-- This test identifies funds where the reported called capital from fund admin
-- differs from the calculated called capital (from cashflows) by more than 0.1%

select
    fund_id,
    fund_name,
    period_end_date,
    called_capital_reported,
    called_capital_calculated,
    called_capital_variance,
    called_capital_variance_pct
from {{ ref('metrics_fund_performance') }}
where abs(called_capital_variance_pct) > 0.001
    and called_capital_reported is not null
    and called_capital_calculated is not null
    and fund_type = 'EQUITY'
