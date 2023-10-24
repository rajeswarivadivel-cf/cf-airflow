with
datalake as (
    select *
    from
        {{ ref('stg_mssql__analytics_dbo_tbldatalake') }}
),

charges as (
    select
        tcap.principalid,
        tc.incomevariable as incomevariable,
        tc.incomefixedfee as incomefixedfee,
        tc.transactionfeegbp as transactionfeegbp,
        tc.markupvariable as markupvariable,
        tc.markupfixedfee as markupfixedfee,
        tc.markupamountgbp as markupamountgbp
    from
        {{ ref('stg_mssql__accounts_dbo_tbltransactioncapture') }} as tcap
    left join
        {{ ref('stg_mssql__accounts_dbo_tbltransactioncharge') }} as tc
        on tcap.transactioncaptureid = tc.transactioncaptureid
),

fx as (
    select
        t.principalid,
        fx.rate
    from
        {{ ref('stg_mssql__accounts_dbo_tbltransaction') }} as t
    left join {{ ref('stg_mssql__accounts_dbo_fx_rates') }} as fx
        on
            lower(t.authcurrency) = lower(fx.from_currency)
            and lower(fx.to_currency) = 'gbp'
            and fx.rate_date = cast(t.starttime as date)
),

cards as (
    select
        te.principalid,
        te.scheme,
        te.afs,
        te.productid,
        tpm.amscardcategory
    from
        {{ ref('stg_mssql__accounts_dbo_tbltransactioneditcriteria') }} as te
    left join
        {{ ref('stg_mssql__accounts_dbo_tlkpproductcard') }} as tpc
        on
            te.scheme = tpc.scheme
            and te.afs = tpc.afs
            and te.productid = replace(tpc.productid, '^', '')
            and coalesce(te.productsubtype, '')
            = coalesce(tpc.productsubtype, '')
    left join
        {{ ref('stg_mssql__accounts_dbo_tlkpproductmapping') }} as tpm
        on tpc.cfcardproduct = tpm.cfcardproduct
),

t3ds as (
    select
        principalid,
        protocolversion
    from
        {{ ref('stg_mssql__accounts_dbo_tbltransaction3dsecure') }}
),

mbl as (
    select *
    from
        {{ ref('stg_mssql__analytics_dev_dbo_vw_masterbidlist') }}
),

joined as (
    select
        dl.postingtime,
        dl.businessid,
        dl.businessname,
        dl.accountnumber,
        dl.partnername,
        mbl.superpartner,
        dl.firsttransactiondate,
        dl.mcc,
        dl.pricingplan,
        dl.regionality,
        dl.scheme,
        dl.transactiontype,
        dl.transactionstatus,
        case
            when c.scheme = 'AM' then 'AMEX'
            when
                c.scheme = 'VI' and c.afs = 'C' and c.productid is null
                then 'CONCRED'
            when
                c.scheme = 'MC' and c.afs = 'C' and c.productid is null
                then 'CONCRED'
            else c.amscardcategory
        end as card_category,
        case
            when t3ds.protocolversion is not null then 'SEC'
            else 'NONSEC'
        end as secure_flag,
        coalesce(dl.transactionamountgbp, 0) as transaction_amount_gbp,
        coalesce(dl.incomeamountgbp, 0) as income_amount_gbp,
        coalesce(dl.interchangegbp, 0) as interchange_gbp,
        coalesce(dl.schemefeesgbp, 0) as scheme_fees_gbp_original,
        coalesce(dl.nonsales_schemefeesgbp, 0) as non_sales_scheme_fees_gbp,
        coalesce(charge.incomevariable, 0) / 100 as income_variable,
        coalesce(charge.transactionfeegbp, 0) as transaction_fee_gbp,
        coalesce(charge.markupvariable, 0) / 100 as markup_variable,
        round(
            coalesce(charge.markupfixedfee, 0) * fx.rate, 2
        ) as markup_fee_gbp,
        coalesce(charge.markupamountgbp, 0) as markup_amount_gbp,
        round(coalesce(incomefixedfee, 0) * fx.rate, 2) as income_fixed_fee_gbp
    from
        datalake as dl
    left join charges as charge
        on dl.principalid = charge.principalid
    left join fx
        on dl.principalid = fx.principalid
    left join cards as c
        on dl.principalid = c.principalid
    left join t3ds
        on dl.principalid = t3ds.principalid
    left join mbl
        on dl.businessid = mbl.bid
),

scheme_fees_gbp_added as (
    select
        *,
        scheme_fees_gbp_original
        + non_sales_scheme_fees_gbp as scheme_fees_gbp
    from joined
),

net_revenue_gbp_added as (
    select
        *,
        income_amount_gbp
        - interchange_gbp
        - scheme_fees_gbp
        - non_sales_scheme_fees_gbp as net_revenue_gbp
    from scheme_fees_gbp_added
),

msc_margin_gbp_added as (
    select
        *,
        net_revenue_gbp
        - transaction_fee_gbp
        - markup_amount_gbp as msc_margin_gbp
    from net_revenue_gbp_added
),

aggregated as (
    select
        extract(year from postingtime) as year,
        extract(month from postingtime) as month,
        businessid as business_id,
        businessname as business_name,
        accountnumber as account_number,
        partnername as partner_name,
        superpartner as super_partner,
        firsttransactiondate as first_transaction_date,
        mcc,
        pricingplan as pricing_plan,
        regionality,
        scheme,
        transactiontype as transaction_type,
        transactionstatus as transaction_status,
        card_category as card_category,
        secure_flag as secure_flag,
        income_variable,
        income_fixed_fee_gbp,
        transaction_fee_gbp as transaction_fee_dbp,
        markup_variable,
        markup_fee_gbp,
        count(postingtime) as transaction_count,
        sum(transaction_amount_gbp) as turnover_gbp,
        sum(income_amount_gbp) as total_revenue_gbp,
        sum(interchange_gbp) as interchange_gbp,
        sum(scheme_fees_gbp) as scheme_fees_gbp,
        sum(msc_margin_gbp) as msc_margin_gbp,
        sum(transaction_fee_gbp) as transaction_fee_revenue_gbp,
        sum(markup_amount_gbp) as markup_revenue_gbp,
        sum(net_revenue_gbp) as net_revenue_gbp
    from
        msc_margin_gbp_added
    group by
        extract(year from postingtime),
        extract(month from postingtime),
        businessid,
        businessname,
        accountnumber,
        partnername,
        superpartner,
        firsttransactiondate,
        mcc,
        pricingplan,
        regionality,
        scheme,
        transactiontype,
        transactionstatus,
        card_category,
        secure_flag,
        income_variable,
        income_fixed_fee_gbp,
        transaction_fee_gbp,
        markup_variable,
        markup_fee_gbp
),

ordered as (
    select *
    from
        aggregated
    order by
        year asc,
        month asc,
        business_id asc,
        business_name asc,
        account_number asc,
        partner_name asc,
        turnover_gbp desc,
        regionality asc,
        scheme asc
)

select *
from
    ordered
