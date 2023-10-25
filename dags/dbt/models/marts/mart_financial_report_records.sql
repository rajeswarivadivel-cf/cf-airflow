with es as (
    select
        tx.transactionid as transactionid,
        tx.authamountgbp,
        tx.authamount,
        tch.totalincomegbp,
        tch.totalincome,
        tch.totalincomegbp - tch.markupamountgbp as incomewithoutmarkupgbp,
        tch.totalincome - tch.markupamount as incomewithoutmarkup,
        tch.markupamountgbp,
        tch.markupamount
    from
        {{ ref('stg_mssql__accounts_dbo_tbltransaction') }} as tx
    inner join {{ ref('stg_mssql__accounts_dbo_tbltransactioncapture') }} as tca
        on
            tx.principalid = tca.principalid
    inner join {{ ref('stg_mssql__accounts_dbo_tbltransactioncharge') }} as tch
        on
            tca.transactioncaptureid = tch.transactioncaptureid
    where
        tch.markupamount > 0
        and tx.transactiontype = 1
),

joined as (
    select
        postingtime,
        businessid,
        businessname,
        partnername,
        '' as relationship,
        coalesce(mml.channel, '') as channel,
        crmid,
        pricingplan,
        settlementtype,
        dl.mid,
        mcc,
        firsttransactiondate,
        merchantcountrycode,
        merchantcountry,
        cardissuecountrycode,
        cardissuecountry,
        transactionclass,
        transactioncurrency,
        regionality,
        scheme,
        product,
        cardproduct,
        coalesce(mbs.partner_manager_name, '') as salesperson,
        coalesce(c.credit_debit, '') as credit_debit,
        coalesce(c.consumer_commercial, '') as consumer_commercial,
        leh.legal_entity,
        case
            when
                transactiontype in (
                    'Sale',
                    'Sale with Cashback'
                )
                and transactionstatus in (
                    'Commissioned',
                    'Paid',
                    'Reserved'
                )
                then 1
            else 0
        end as is_in_scope,
        case
            when
                transactiontype in (
                    'Sale',
                    'Sale with Cashback'
                )
                and transactionstatus in (
                    'Commissioned',
                    'Paid',
                    'Reserved'
                )
                then transactionamount
            else 0
        end as totalturnover,
        case
            when
                transactiontype in (
                    'Sale',
                    'Sale with Cashback'
                )
                and transactionstatus in (
                    'Commissioned',
                    'Paid',
                    'Reserved'
                )
                then transactionamountgbp
            else 0
        end as totalturnovergbp,
        totalrevenue,
        totalrevenuegbp,
        case
            when
                transactiontype in (
                    'Sale',
                    'Sale with Cashback'
                )
                and transactionstatus in (
                    'Commissioned',
                    'Paid',
                    'Reserved'
                )
                then incomeamount
            else 0
        end as msc,
        case
            when
                transactiontype in (
                    'Sale',
                    'Sale with Cashback'
                )
                and transactionstatus in (
                    'Commissioned',
                    'Paid',
                    'Reserved'
                )
                then incomeamountgbp
            else 0
        end as mscgbp,
        case
            when
                transactionstatus in (
                    'Pending',
                    'Reserved',
                    'Commissioned',
                    'Paid'
                )
                then transactionfee
            else 0
        end as authfees,
        case
            when
                transactionstatus in (
                    'Pending',
                    'Reserved',
                    'Commissioned',
                    'Paid'
                )
                then transactionfeegbp
            else 0
        end as authfeesgbp,
        case
            when
                transactionstatus in (
                    'Rejected',
                    'Cancelled',
                    'Failed',
                    'Expired',
                    'Blocked',
                    'Declined'
                )
                then
                    case
                        when incomeamount = 0 then transactionfee else
                            incomeamount
                    end
            else 0
        end as declinefees,
        case
            when
                transactionstatus in (
                    'Rejected',
                    'Cancelled',
                    'Failed',
                    'Expired',
                    'Blocked',
                    'Declined'
                )
                then
                    case
                        when incomeamountgbp = 0 then transactionfeegbp else
                            incomeamountgbp
                    end
            else 0
        end as declinefeesgbp,
        case
            when
                transactiontype in (
                    'Chargeback',
                    'Refund Chargeback',
                    'Second Chargeback',
                    'Dispute',
                    'Representment',
                    'Dispute Response',
                    'Pre-Arbitration',
                    'Dispute Reversal',
                    'Liability Assignment',
                    'Chargeback Reversal'
                ) then incomeamount
            else 0
        end as chargebackrevenue,
        case when transactiontype in (
            'Chargeback',
            'Refund Chargeback',
            'Second Chargeback',
            'Dispute',
            'Dispute Response',
            'Pre-Arbitration',
            'Dispute Reversal',
            'Liability Assignment',
            'Chargeback Reversal'
        ) then incomeamountgbp
        else 0 end as chargebackrevenuegbp,
        case
            when
                transactiontype in ('High-Risk Warning')
                then incomeamount
            else 0
        end as hrwrevenue,
        case
            when
                transactiontype in ('High-Risk Warning')
                then incomeamountgbp
            else 0
        end as hrwrevenuegbp,
        case
            when
                transactiontype in (
                    'Refund',
                    'Refund Reversal'
                )
                then incomeamount
            else 0
        end as refundrevenue,
        case
            when
                transactiontype in (
                    'Refund',
                    'Refund Reversal'
                )
                then incomeamountgbp
            else 0
        end as refundrevenuegbp,
        case
            when
                transactiontype in ('Credit Transfer')
                then incomeamount else
                0
        end as creditrevenue,
        case
            when
                transactiontype in ('Credit Transfer')
                then incomeamountgbp
            else 0
        end as creditrevenuegbp,
        case
            when
                transactiontype in ('Copy Request')
                then incomeamount
            else 0
        end as copyrevenue,
        case
            when
                transactiontype in ('Copy Request')
                then incomeamountgbp
            else 0
        end as copyrevenuegbp,
        case
            when
                transactiontype in (
                    'Sale',
                    'Sale with Cashback',
                    'Refund'
                )
                and transactionstatus in (
                    'Commissioned',
                    'Paid',
                    'Reserved'
                )
                then interchange
            else 0
        end as interchange,
        case
            when
                transactiontype in (
                    'Sale',
                    'Sale with Cashback',
                    'Refund'
                )
                and transactionstatus in (
                    'Commissioned',
                    'Paid',
                    'Reserved'
                )
                then interchangegbp
            else 0
        end as interchangegbp,
        isnull(schemefees, 0) as schemefeessale,
        isnull(schemefeesgbp, 0) as schemefeessalegbp,
        isnull(dl.nonsales_schemefees, 0) as schemefeesnonsale,
        isnull(dl.nonsales_schemefeesgbp, 0) as schemefeesnonsalegbp,
        isnull(customergp, 0) as customergp,
        isnull(customergpgbp, 0) as customergpgbp,
        isnull(customergppercent, 0) as customergppercent,
        isnull(cashflowsgppercent, 0) as cashlowsgppercent,
        isnull(es.authamountgbp, 0) as esauthamountgbp,
        isnull(es.totalincomegbp, 0) as estotalincomegbp,
        isnull(es.incomewithoutmarkupgbp, 0) as esincomewithoutmarkupgbp,
        isnull(es.markupamountgbp, 0) as esmarkupamountgbp
    from {{ ref('stg_mssql__analytics_dbo_tbldatalake') }} as dl
    left outer join
        es
        on dl.transactionid = es.transactionid
    left outer join
        {{ ref('stg_mssql__analytics_dbo_tblbidbysalesperson_history') }} as mbs
        on
            mbs.bid = businessid
            and cast(postingtime as date) between mbs.startdate and coalesce(
                mbs.enddate, '2099-12-31'
            )
    left outer join
        {{ ref('stg_mssql__analytics_dev_dbo_vw_mastermidlist') }} as mml
        on dl.mid = mml.mid
    left join
        {{ ref('card_types') }} as c
        on c.card_name = cardproduct
    left outer join
        {{ ref('stg_mssql__analytics_dbo_tbllegalentity_by_bid_history') }} as leh
        on
            leh.bid = businessid
            and dl.postingtime between leh.startdate and isnull(
                leh.enddate, '2099-12-31'
            )
),

final as (
    select
        cast(postingtime as date) as transactiondate,
        businessid,
        businessname,
        partnername,
        '' as relationship,
        channel,
        crmid,
        pricingplan,
        settlementtype,
        dl.mid,
        mcc,
        firsttransactiondate,
        merchantcountrycode,
        merchantcountry,
        cardissuecountrycode,
        cardissuecountry,
        transactionclass,
        transactioncurrency,
        regionality,
        scheme,
        product,
        cardproduct,
        salesperson,
        credit_debit,
        consumer_commercial,
        legal_entity,
        sum(is_in_scope) as transactioncount,
        sum(totalturnover) as totalturnover,
        sum(totalrevenue) as totalrevenue,
        sum(msc) as msc,
        sum(authfees) as authfees,
        sum(declinefees) as declinefees,
        sum(chargebackrevenue) as chargebackrevenue,
        sum(hrwrevenue) as hrwrevenue,
        sum(refundrevenue) as refundrevenue,
        sum(creditrevenue) as creditrevenue,
        sum(copyrevenue) as copyrevenue,
        sum(interchange) as interchange,
        sum(schemefeessale) as schemefeessale,
        sum(schemefeesnonsale) as schemefeesnonsale,
        sum(customergp) as customergp,
        avg(customergppercent) as customergppercent,
        avg(cashlowsgppercent) as cashlowsgppercent,
        sum(totalturnovergbp) as totalturnovergbp,
        sum(totalrevenuegbp) as totalrevenuegbp,
        sum(mscgbp) as mscgbp,
        sum(authfeesgbp) as authfeesgbp,
        sum(declinefeesgbp) as declinefeesgbp,
        sum(chargebackrevenuegbp) as chargebackrevenuegbp,
        sum(hrwrevenuegbp) as hrwrevenuegbp,
        sum(refundrevenuegbp) as refundrevenuegbp,
        sum(creditrevenuegbp) as creditrevenuegbp,
        sum(copyrevenuegbp) as copyrevenuegbp,
        sum(interchangegbp) as interchangegbp,
        sum(schemefeessalegbp) as schemefeessalegbp,
        sum(schemefeesnonsalegbp) as schemefeesnonsalegbp,
        sum(customergpgbp) as customergpgbp,
        sum(esauthamountgbp) as esauthamountgbp,
        sum(estotalincomegbp) as estotalincomegbp,
        sum(esincomewithoutmarkupgbp) as esincomewithoutmarkupgbp,
        sum(esmarkupamountgbp) as esmarkupamountgbp
    from joined as dl
    {{ dbt_utils.group_by(n=26) }}
    order by
        cast(postingtime as date),
        businessid,
        businessname,
        partnername,
        crmid,
        pricingplan,
        settlementtype,
        mid,
        mcc,
        firsttransactiondate,
        merchantcountrycode,
        merchantcountry,
        cardissuecountrycode,
        cardissuecountry,
        transactioncurrency,
        transactionclass,
        regionality,
        scheme,
        product,
        cardproduct
)

select * from final
