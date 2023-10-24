with
sales as (
    select *
    from
        {{ ref('stg_mssql__accounts_dbo_acc_transactions') }}
    where
        event_type_id = 0
),

chargebacks as (
    select *
    from
        {{ ref('stg_mssql__accounts_dbo_acc_transactions') }}
    where
        event_type_id = 11
),

reversals as (
    select *
    from
        {{ ref('stg_mssql__accounts_dbo_acc_transactions') }}
    where
        event_type_id = 12
),

representments as (
    select *
    from
        {{ ref('stg_mssql__accounts_dbo_acc_transactions') }}
    where
        event_type_id = 13
),

second_chargebacks as (
    select *
    from
        {{ ref('stg_mssql__accounts_dbo_acc_transactions') }}
    where
        event_type_id = 61
),

joined as (
    select
        c.external_ref as chargeback_reference,
        s.description as sale_description,
        coalesce(s.amount_minor_units / power(10, 2), 0) as sale_amount,
        s.transaction_time as sale_created_at,
        s.source_ref as mid,
        c.description as chargeback_description,
        c.transaction_time as chargeback_created_at,
        rep.description as representment_description,
        rep.transaction_time as representment_created_at,
        sec.description as second_chargeback_description,
        sec.transaction_time as second_chargeback_created_at,
        rev.description as reversal_description,
        rev.transaction_time as reversal_created_at,
        td.cust_email
    from
        sales as s
    left join chargebacks as c
        on s.transaction_id = c.txn_first_id
    left join reversals as rev
        on s.transaction_id = rev.txn_first_id
    left join representments as rep
        on s.transaction_id = rep.txn_first_id
    left join second_chargebacks as sec
        on s.transaction_id = sec.txn_first_id
    left join {{ ref('stg_mssql__core_dbo_trans_details') }} as td
        on s.external_ref = td.tran_ref
)

select *
from
    joined
