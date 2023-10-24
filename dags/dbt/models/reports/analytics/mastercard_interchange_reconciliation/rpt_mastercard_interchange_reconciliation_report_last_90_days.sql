select *
from {{ ref('int_mastercard_interchange_reconciliation_grouped') }}
where
    transaction_posted_on >= current_date - interval '90 days'
