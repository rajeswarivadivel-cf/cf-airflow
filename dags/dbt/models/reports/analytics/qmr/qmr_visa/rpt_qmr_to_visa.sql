with

visa_quarterly as (
    select distinct
        a.postingtime as posting_time,
        substring(arn, 2, 6) as acq_bin_6,
        f.legal_entity,
        a.transactionid,
        a.scheme,
        a.merchantcountrycode,
        a.merchantcountry,
        a.cardissuecountrycode,
        a.cardissuecountry,
        a.transactiontype,
        a.transactionstatus,
        a.transactioncurrency,
        b.productid,
        b.afs,
        a.mcc,
        a.txtogbpfxrate,
        a.relationship,
        a.transactionclass,
        a.threedsecureprotocol,
        product,
        case
            when c.trans_class_id = 6 then 'Credit Transfer'
            when c.trans_class_id in (1, 2, 3)
                then 'CNP'
            else 'CP'
        end as cp_cnp_flag,
        case
            when
                a.merchantcountry = a.cardissuecountry
                or (
                    lower(mc.jurisdiction) = 'domestic'
                    and lower(cic.jurisdiction) = 'domestic'
                ) then 'Domestic'
            when
                lower(mc.jurisdiction) = 'europe_eea'
                and lower(cic.jurisdiction) = 'europe_eea'
                then 'Europe_EEA'
            when (
                lower(mc.jurisdiction) in (
                    'domestic', 'europe_others'
                )
                and cic.jurisdiction in (
                    'domestic', 'europe_eea', 'europe_others'
                )
            ) or (
                lower(mc.jurisdiction) in (
                    'domestic', 'europe_eea', 'europe_others'
                )
                and cic.jurisdiction in (
                    'domestic', 'europe_others'
                )
            ) then 'Europe_Non_EEA'
            else 'International'
        end as visa_sf_jurisdiction,
        case
            when mc.is_micro_state then 'micro_state'
        end as ms_flag,
        case
            when
                a.mcc in (
                    '4111',
                    '4112',
                    '4131',
                    '4900',
                    '5411',
                    '5541',
                    '5542',
                    '8398',
                    '9311'
                )
                then 'everydayspend'
            else 'standard'
        end as everyday_spend,
        fxt.rate,
        a.transactionamount * fx.rate as transactionamounteur,
        a.transactionamountgbp,
        cb.cashback_value,
        cb.currency_code as cashback_currency_code
    from analytics.mssql__analytics_dbo_tbldatalake as a
    left join
        analytics.mssql__accounts_dbo_tbltransactioneditcriteria as b
        on a.principalid = b.principalid
    left join
        analytics.mssql__core_dbo_transaction_class as c
        on a.transactionclass = c.trans_class_description
    left join
        analytics.mssql__core_dbo_trans_details as d
        on a.businessid = d.tran_ref
    left join
        analytics.mssql__analytics_dev_dbo_vw_masterbidlist as f
        on a.businessid = f.bid
    left join
        analytics.mssql__accounts_dbo_fx_rates as fxt
        on
            fxt.rate_date = cast(a.postingtime as date)
            and lower(fxt.from_currency) = 'eur'
            and lower(fxt.to_currency) = 'gbp'
    left join
        analytics.mssql__accounts_dbo_fx_rates as fx
        on
            fx.rate_date = cast(a.postingtime as date)
            and a.transactioncurrency = fx.from_currency
            and lower(fx.to_currency) = 'eur'
    left join
        analytics.rpt_visa_qmr_to_countries as mc
        on lower(a.merchantcountry) = lower(mc.country)
    left join
        analytics.rpt_visa_qmr_to_countries as cic
        on lower(a.cardissuecountry) = lower(cic.country)
    left join
        analytics.int_qmr_cashbacks as cb
        on a.transactionid = cb.transactionid
    where
        cast(a.postingtime as date) between '2023-08-01' and '2023-08-31'
        and lower(a.scheme) = 'visa'
        and lower(a.transactiontype) in (
            'sale', 'representment', 'sale with cashback'
        )
        and lower(a.transactionstatus) in ('paid', 'reserved', 'commissioned')
),

maps_added as (
    select
        *,
        case
            when lower(afs) = 'c' then 'credit'
            when lower(afs) in ('h', 'r') then 'deferred debit/charge'
            when lower(afs) = 'd' then 'debit'
            when lower(afs) = 'p' then 'prepaid'
        end as qoc_afs_map,
        case
            when
                lower(productid) in
                (
                    'a',
                    'b',
                    'c',
                    'd',
                    'f',
                    'f2',
                    'i',
                    'i1',
                    'i2',
                    'j3',
                    'l',
                    'n',
                    'n1',
                    'n2',
                    'p',
                    'u',
                    'v'
                )
                then 'Consumer'
            when
                lower(productid) in
                (
                    'k',
                    'k1',
                    's',
                    's1',
                    's2',
                    's3',
                    's4',
                    's5',
                    'x',
                    'x1'
                )
                then 'Commercial'
            when
                lower(productid) in
                (
                    'g',
                    'g1',
                    'g3',
                    'g4',
                    'g5',
                    's6'
                )
                then 'Business'
            when
                lower(productid) in
                (
                    'q',
                    'q2',
                    'q3',
                    'q4',
                    'q5',
                    'q6'
                )
                then 'Private Label'
        end as visa_product_crdb_mapping
    from visa_quarterly
),

aggregated as (
    select
        extract(year from posting_time) as posting_year,
        extract(quarter from posting_time) as posting_quarter,
        extract(month from posting_time) as posting_month,
        acq_bin_6,
        legal_entity,
        scheme,
        merchantcountrycode as merchant_country_code,
        merchantcountry as merchant_country,
        cardissuecountrycode as card_issue_country_code,
        cardissuecountry as card_issue_country,
        transactiontype as transaction_type,
        productid as product_id,
        afs,
        qoc_afs_map,
        visa_product_crdb_mapping,
        mcc,
        product,
        cp_cnp_flag,
        visa_sf_jurisdiction,
        ms_flag,
        everyday_spend,
        cashback_currency_code,
        count(transactionid) as transaction_volume,
        sum(transactionamountgbp) as transaction_amount_gbp,
        sum(transactionamounteur) as transaction_amount_eur,
        sum(cashback_value) as cashback_value
    from maps_added
    {{ dbt_utils.group_by(n=22) }}
)

select * from aggregated
