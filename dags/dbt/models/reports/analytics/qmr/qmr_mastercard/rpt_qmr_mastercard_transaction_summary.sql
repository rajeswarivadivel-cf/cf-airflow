{% set DOMESTIC = 'Domestic' %}
{% set INTRA_SEPA = 'INTRA_SEPA' %}
{% set INTRA_NON_SEPA = 'INTRA_NON_SEPA' %}
{% set INTER_EUROPEAN = 'INTER_EUROPEAN' %}
{% set INTER_REGIONAL = 'INTER_REGIONAL' %}
{% set MAESTRO = 'MAESTRO' %}

with

cashbacks as (
    select
        s.reference as transactionid,
        s.currency_code,
        cast(
            cast(isnull(s.other_amounts_transaction, 0) as float (53))
            / power(10, cc.minor_unit_precision) as decimal(18, 2)
        ) as cashback_value
    from analytics.mssql__core_dbo_sales as s
    left join
        analytics.mssql__core_dbo_currencies as cc
        on s.currency_code = cc.currency_code
    where
        s.other_amounts_transaction is not null
        and s.other_amounts_transaction != 0.00
),

joined as (
    select distinct
        dl.postingtime,
        dl.transactionid,
        dl.scheme,
        dl.merchantcountrycode,
        dl.merchantcountry,
        dl.cardissuecountry,
        dl.transactiontype,
        dl.transactionstatus,
        dl.mcc,
        dl.relationship,
        dl.transactionclass,
        dl.transactionamount,
        dl.transactionamountgbp,
        tec.productid,
        tec.productsubtype,
        tec.afs,
        tc.trans_class_id,
        bid.legal_entity,
        sfjm.sepa as is_sepa_merchant,
        sfjc.sepa as is_sepa_card_issuer,
        bin.mscd_bin,
        bin.mscd_acquirer_ica,
        to_gbp_fx.rate as to_gbp_fx_rate,
        cb.cashback_value,
        cb.currency_code as cashback_currency_code,
        p.consumer_or_commercial,
        p.debit_or_credit,
        p.is_maestro
    from
        {{ ref('stg_mssql__analytics_dbo_tbldatalake') }} as dl
    left join
        {{ ref('stg_mssql__accounts_dbo_tbltransactioneditcriteria') }} as tec
        on dl.principalid = tec.principalid
    left join {{ ref('rpt_mastercard_qmr_to_product_ids') }} as p
        on lower(tec.productid) = lower(p.product_id)
    left join
        {{ ref('stg_mssql__core_dbo_transaction_class') }} as tc
        on dl.transactionclass = tc.trans_class_description
    left join
        {{ ref('stg_mssql__core_dbo_trans_details') }} as td
        on dl.businessid = td.tran_ref
    left join
        {{ ref('stg_mssql__analytics_dev_dbo_vw_masterbidlist') }} as bid
        on dl.businessid = bid.bid
    left join
        {{ ref('stg_scheme_fee_jurisdictions') }} as sfjm
        on dl.merchantcountry = sfjm.country_name
    left join
        {{ ref('stg_scheme_fee_jurisdictions') }} as sfjc
        on dl.cardissuecountry = sfjc.country_name
    left join
        {{ ref('stg_mssql__core_dbo_acquirer_bin') }} as bin
        on substring(dl.arn, 2, 6) = bin.mscd_bin
    left join
        {{ ref('stg_mssql__accounts_dbo_fx_rates') }} as to_gbp_fx
        on
            cast(dl.postingtime as date) = to_gbp_fx.rate_date
            and dl.transactioncurrency = to_gbp_fx.from_currency
    left join
        cashbacks as cb
        on dl.transactionid = cb.transactionid
    where
        lower(dl.transactiontype) in (
            'sale', 'representment', 'sale with cashback', 'refund'
        )
        and lower(dl.transactionstatus) in ('paid', 'reserved', 'commissioned')
        and tc.trans_class_id != 6
        and lower(dl.scheme) = 'mastercard'
        and sfjm.mc
        and sfjc.mc
        and lower(to_gbp_fx.to_currency) = 'eur'
),

columns_added as (
    select
        *,
        transactionamount * to_gbp_fx_rate as transaction_amount_eur,
        case
            when trans_class_id = 6 then 'CREDIT TRANSFER'
            when trans_class_id in (1, 2, 3) then 'CNP'
            else 'CP'
        end as cp_cnp_flag,
        case
            when
                merchantcountry = cardissuecountry
                then '{{ DOMESTIC }}'
            when
                is_sepa_merchant and is_sepa_card_issuer
                then '{{ INTRA_SEPA }}'
            when
                not is_sepa_merchant and not is_sepa_card_issuer
                then '{{ INTRA_NON_SEPA }}'
            when
                is_sepa_merchant and not is_sepa_card_issuer
                then '{{ INTER_EUROPEAN }}'
            when
                not is_sepa_merchant and is_sepa_card_issuer
                then '{{ INTER_EUROPEAN }}'
            else '{{ INTER_REGIONAL }}'
        end as mc_sf_jurisdiction_1,
        case
            when
                merchantcountry = cardissuecountry
                then '{{ DOMESTIC }}'
            when
                is_sepa_merchant and is_sepa_card_issuer
                then '{{ INTER_EUROPEAN }}'
            when
                not is_sepa_merchant and not is_sepa_card_issuer
                then '{{ INTER_EUROPEAN }}'
            when
                is_sepa_merchant and not is_sepa_card_issuer
                then '{{ INTER_EUROPEAN }}'
            when
                not is_sepa_merchant and is_sepa_card_issuer
                then '{{ INTER_EUROPEAN }}'
            else '{{ INTER_REGIONAL }}'
        end as mc_sf_jurisdiction_2,
        coalesce(consumer_or_commercial, 'CONSUMER') as consumer_commercial,
        case
            when lower(debit_or_credit) = 'debit' then 'DEBIT'
            else 'CREDIT'
        end as debit_credit,
        case
            when is_maestro then '{{ MAESTRO }}'
            else ''
        end as maestro_flag,
        case
            when
                lower(legal_entity) = 'cfe' and not is_maestro
                then '13908'
            when
                lower(legal_entity) = 'cfe' and is_maestro
                then '83317'
            when
                lower(legal_entity) = 'pco' and not is_maestro
                then '19748'
            when
                lower(legal_entity) = 'cfe' and is_maestro
                then '81347'
        end as ica,
        case
            when
                mc_sf_jurisdiction_1 = '{{ DOMESTIC }}'
                then 'Domestic Interchange Acquiring'
            when
                mc_sf_jurisdiction_1 = '{{ INTRA_SEPA }}'
                then 'International Acquiring Within Region'
            else 'International Acquiring Outside of Region'
        end as qmr_reporting_bucket
    from joined
),

final as (
    select
        extract(year from postingtime) as posting_year,
        extract(quarter from postingtime) as posting_quarter,
        extract(month from postingtime) as posting_month,
        mscd_bin,
        mscd_acquirer_ica,
        legal_entity,
        ica,
        scheme,
        merchantcountrycode as merchant_country_code,
        merchantcountry as merchant_count,
        transactiontype as transaction_type,
        transactionstatus as transaction_staus,
        productid as product_id,
        productsubtype as product_subtype,
        afs,
        mcc,
        relationship,
        transactionclass as transaction_class,
        cp_cnp_flag,
        mc_sf_jurisdiction_1,
        mc_sf_jurisdiction_2,
        qmr_reporting_bucket,
        consumer_commercial,
        debit_credit,
        maestro_flag,
        cashback_currency_code,
        count(transactionid) as transaction_count,
        sum(transactionamountgbp) as transaction_amount_gbp,
        sum(transaction_amount_eur) as transaction_amount_eur,
        sum(cashback_value) as cashback_value
    from columns_added
    {{ dbt_utils.group_by(n=26) }}
)

select * from final
