with
sales as (
    select *
    from
        {{ ref('stg_mssql__accounts_dbo_acc_transactions') }}
    where
        event_type_id = 0
),

disputes as (
    select *
    from
        {{ ref('stg_mssql__accounts_dbo_acc_transactions') }}
    where
        event_type_id = 71
),

responses as (
    select *
    from
        {{ ref('stg_mssql__accounts_dbo_acc_transactions') }}
    where
        event_type_id = 73
),

pre_arbitrations as (
    select *
    from
        {{ ref('stg_mssql__accounts_dbo_acc_transactions') }}
    where
        event_type_id = 74
),

reversals as (
    select *
    from
        {{ ref('stg_mssql__accounts_dbo_acc_transactions') }}
    where
        event_type_id = 77
),

arbitrations as (
    select *
    from
        {{ ref('stg_mssql__accounts_dbo_acc_transactions') }}
    where
        event_type_id = 79
),

clearing_reasons as (
    select
        sar.id,
        rc.description as clearing_reason_codes_description
    from
        {{ ref('stg_mssql__core_dbo_sale_auth_responses') }} as sar
    left join {{ ref('stg_mssql__core_dbo_saledisputes') }} as sd
        on sar.acq_ref_num = sd.orig_sale_arn
    left join {{ ref('stg_mssql__core_dbo_clearing_reason_codes') }} as rc
        on sd.reason = rc.reason_code
    left join {{ ref('stg_mssql__core_dbo_clearing_reason_sub_codes') }} as rcs
        on
            sd.dispute_condition = rcs.reason_sub_code
            and sd.reason = rcs.reason_code
    where
        rc.tran_type = 45
        and rcs.tran_type = 45
),

transaction_details as (
    select
        s.reference,
        td.cust_email,
        cr.clearing_reason_codes_description
    from
        {{ ref('stg_mssql__core_dbo_sales') }} as s
    left join clearing_reasons as cr
        on s.id = cr.id
    left join {{ ref('stg_mssql__core_dbo_trans_details') }} as td
        on s.reference = td.tran_ref
),

joined as (
    select
        d.external_ref as dispute_reference,
        s.description as sale_description,
        coalesce(s.amount_minor_units / power(10, 2), 0) as sale_amount,
        s.transaction_time as sale_created_at,
        s.source_ref as mid,
        d.description as dispute_description,
        d.transaction_time as dispute_created_at,
        res.description as response_description,
        res.transaction_time as response_created_at,
        rev.description as reversal_description,
        rev.transaction_time as reversal_created_at,
        pa.description as pre_arbitration_description,
        pa.transaction_time as pre_arbitration_created_at,
        td.cust_email as customer_email
    from
        sales as s
    left join disputes as d
        on s.transaction_id = d.txn_first_id
    left join responses as res
        on s.transaction_id = res.txn_first_id
    left join pre_arbitrations as pa
        on s.transaction_id = pa.txn_first_id
    left join reversals as rev
        on s.transaction_id = rev.txn_first_id
    left join arbitrations as a
        on s.transaction_id = a.txn_first_id
    left join transaction_details as td
        on s.external_ref = td.reference
    where
        dispute_description is not null
        and lower(td.clearing_reason_codes_description) not like '%fraud%'
)

select *
from
    joined
