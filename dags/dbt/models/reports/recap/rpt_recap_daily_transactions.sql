with
transactions as (
    select *
    from
        {{ ref('stg_mssql__analytics_dbo_tbldatalake') }}
    where
        lower(relationship) = 'direct'
        and lower(merchantcountrycode) = 'gb'
        and businessid != 444595
        and lower(transactiontype) in (
            'sale', 'sale with cashback', 'refund', 'representment'
        )
        and lower(transactionstatus) in ('paid', 'reserved', 'commissioned')
        and mcc != '4121'
        and lower(product) = 'cnp'
),

last_activity_dates as (
    select
        mid,
        max(cast(postingtime as date)) as last_activity_date
    from
        {{ ref('stg_mssql__analytics_dbo_tbldatalake') }}
    where
        cast(postingtime as date) >= '2023-01-01'
    group by
        mid
),

merchant_proprietor_types as (
    select distinct
        s.store_id as mid,
        m.merchant_proprietor_type
    from
        {{ ref('stg_mssql__core_dbo_terminals') }} as t
    left join
        {{ ref('stg_mssql__core_dbo_stores') }} as s
        on
            cast(s.store_id as varchar) = t.mid
            or cast(s.merchant_id as varchar) = t.mid
    left join
        {{ ref('stg_mssql__core_dbo_merchants') }} as m
        on s.merchant_id = m.merchant_id
    where
        s.status = 2 -- 2: Live
        and t.status = 1 -- 1: Live, 2: Not live
        and lower(t.country_code) = 'gb'
),

joined as (
    select
        t.transactionid as transaction_id,
        t.transactiontype as transaction_type,
        t.transactionstatus as transaction_status,
        t.postingtime as posting_time,
        t.firsttransactiondate as first_transaction_date,
        t.scheme,
        t.product,
        t.relationship,
        t.mcc,
        t.transactionclass as transaction_class,
        t.merchantcountrycode as merchant_country_code,
        t.merchantcountry as merchant_country,
        t.businessid as business_id,
        t.mid,
        t.terminalid as terminal_id,
        t.transactioncurrency as transaction_currency,
        t.transactionamount as transaction_amount,
        t.transactionamountgbp as transaction_amount_gbp,
        lad.last_activity_date,
        mpt.merchant_proprietor_type,
        curr.settlement_currency
    from
        transactions as t
    left join
        merchant_proprietor_types as mpt
        on t.mid = mpt.mid
    left join last_activity_dates as lad
        on t.mid = lad.mid
    left join
        {{ ref('mart_transaction_settlement_currencies') }} as curr
        on t.transactionid = curr.external_ref
),

aggregated as (
    select
        transaction_type,
        transaction_status,
        cast(posting_time as date) as transaction_date,
        last_activity_date,
        first_transaction_date,
        scheme,
        product,
        relationship,
        mcc,
        transaction_class,
        merchant_country_code,
        merchant_country,
        business_id,
        mid,
        terminal_id,
        transaction_currency,
        merchant_proprietor_type,
        settlement_currency,
        count(transaction_id) as transaction_count,
        sum(transaction_amount) as transaction_amount,
        sum(transaction_amount_gbp) as transaction_amount_gbp
    from
        joined
    {{ dbt_utils.group_by(n=18) }}
)

select *
from
    aggregated
