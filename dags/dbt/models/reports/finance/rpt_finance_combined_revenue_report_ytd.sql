select *
from {{ ref('rpt_finance_combined_revenue_report') }}
where period_start_date >= cast(date_trunc('year', current_date) as date)