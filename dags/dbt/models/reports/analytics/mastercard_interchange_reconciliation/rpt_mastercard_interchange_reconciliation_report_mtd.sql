select *
from {{ ref('int_mastercard_interchange_reconciliation_grouped') }}
where
    date_part(year, transaction_posted_on) = date_part(year, getdate())
    and date_part(month, transaction_posted_on) = date_part(month, getdate())
