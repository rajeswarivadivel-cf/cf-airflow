{% set source %}
select
    *,
    'FINANCIAL_REPORT_RECORD' as source
from
    {{ ref('int_combined_revenue_report_financial_report_records') }}
union distinct
select
    *,
    'NON_SALES_SCHEME_FEE' as source
from
    {{ ref('int_combined_revenue_report_non_sales_scheme_fees') }}
union distinct
select
    *,
    'MANUAL_ADJUSTMENT' as source
from
    {{ ref('int_combined_revenue_report_manual_adjustments') }}
{% endset %}

{{ rpt_combined_revenue_report_base(source=source) }}
