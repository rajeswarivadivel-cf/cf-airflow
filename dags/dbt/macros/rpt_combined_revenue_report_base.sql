{% macro rpt_combined_revenue_report_base(source) %}
with
source as (
    {{ source }}
),

merchants as (
    select
        merchant_id,
        merchant_name
    from
        {{ ref('stg_mssql__core_dbo_merchants') }}
),

bid_master as (
    select
        channel,
        channel2,
        partner_name,
        legal_entity,
        bid,
        partner_id
    from
        {{ ref('stg_mssql__analytics_dev_dbo_vw_masterbidlist') }}
),

bid_mcc as (
    select
        bid,
        mcc,
        description
    from
        {{ ref('stg_mssql__analytics_dev_dbo_vw_bid_mcc') }}
),

joined as (
    select
        to_date(
            year || lpad(cast(month as text), 2, 0) || '01', 'YYYYMMDD'
        ) as period_start_date,
        source.source,
        source.bid,
        source.comments as comment,
        coalesce(source.acqtotalrevenuegbp, 0) as acq_total_revenue_gbp,
        coalesce(source.gatewayrevenuegbp, 0) as gateway_revenue_gbp,
        coalesce(source.pcirevenuegbp, 0) as pci_revenue_gbp,
        coalesce(
            source.accountupdatertotalrevenuegbp, 0
        ) as account_updater_total_revenue_gbp,
        coalesce(source.fxtotalrevenuegbp, 0) as fx_total_revenue_gbp,
        coalesce(
            source.manualearlysettlementgbp, 0
        ) as manual_early_settlement_gbp,
        coalesce(
            source.autoearlysettlementrevenuegbp, 0
        ) as auto_early_settlement_revenue_gbp,
        coalesce(source.totalpartnercommissiongbp, 0) as partner_commission,
        coalesce(source.ic_costs, 0) as interchange_fee,
        coalesce(source.sf_costs, 0) as scheme_fee,
        coalesce(source.revenuegbp, 0) as manual_adjusted_revenue_gbp,
        coalesce(source.thirdpartygatewaycosts, 0) as third_party_gateway_cost,
        coalesce(source.cosgbp, 0) as manual_adjusted_cos_gbp,
        coalesce(source.salescommissions, 0) as sales_commission,
        coalesce(source.totalturnovergbp, 0) as total_turnover_gbp,
        coalesce(source.totalcosgbp, 0) as fx_cost_gbp,
        coalesce(
            source.remittancefeerevenuegbp, 0
        ) as remittance_fee_revenue_gbp,
        coalesce(source.remittancefeecostgbp, 0) as remittance_fee_cost_gbp,
        coalesce(source.transactioncount, 0) as transaction_count,
        coalesce(
            source.value_allocated_scheme_fees, 0
        ) as value_allocated_scheme_fee,
        coalesce(
            source.value_allocated_interchange, 0
        ) as value_allocated_interchange,
        coalesce(source."3ds cost gbp", 0) as "3ds_cost_gbp",
        coalesce(source."3ds revenue gbp", 0) as "3ds_revenue_gbp",
        coalesce(bid.merchant_name, '') as merchant_name,
        coalesce(mbl.channel, '') as channel_original,
        coalesce(mbl.channel2, '') as channel_2,
        mbl.partner_id,
        coalesce(mbl.partner_name, '') as partner_name,
        coalesce(mbl.legal_entity, '') as legal_entity,
        mcc.mcc,
        coalesce(mcc.description, '') as description
    from
        source
    left join merchants as bid
        on source.bid = bid.merchant_id
    left join bid_master as mbl
        on source.bid = mbl.bid
    left join bid_mcc as mcc
        on source.bid = mcc.bid
),

first_transaction_dates as (
    -- This table is supposed to be the same as if generate from accounts.dbo.tblTransaction.
    -- However, variance is found, which probably is due to "occasional archiving and removal of historic AMS data".
    select
        businessid,
        firsttransactiondate
    from
        {{ ref('stg_mssql__analytics_dbo_tblfirsttransactiondate') }}
),

existing_or_new_added as (
    -- If a business has any valid transaction before current year, it is an "existing" business.
    -- Otherwise it is an "new" business.
    -- A valid transaction is one that is with greater than zero turnover.
    select
        t.*,
        f.firsttransactiondate as first_transaction_date,
        case
            when f.firsttransactiondate is null then 'Unknown'
            when
                f.firsttransactiondate < trunc(date_trunc('year', getdate()))
                then 'Existing'
            else 'New'
        end as existing_or_new
    from
        joined as t
    left join first_transaction_dates as f
        on t.bid = f.businessid
),

sales_person as (
    select
        partner_manager_name,
        bid,
        startrange,
        isnull(endrange, cast(getdate() as date)) as endrange
    from
        {{ ref('stg_mssql__analytics_dbo_vw_bidbysalesperson_15thcutoff') }}
),

secondary_sales_person as (
    select
        partner_manager_name_secondary,
        bid,
        startrange,
        isnull(endrange, cast(getdate() as date)) as endrange
    from
        {{ ref('stg_mssql__analytics_dbo_vw_bidbysalesperson_secondary_15thcutoff') }}
),

partner_channel as (
    select
        partner_id,
        channel2,
        to_date(startrange, 'YYYY-MM-DD') as startrange,
        to_date(
            case endrange
                when null then '2100-12-31'
                when '' then '2100-12-31'
                else endrange
            end,
            'YYYY-MM-DD'
        ) as endrange
    from
        {{ ref('stg_mssql__analytics_dbo_vw_partner_channel_15thcutoff') }}
),

managers_added as (
    -- Assign managers based on channel.
    -- Managers of a business is different at different transaction dates.
    select
        t.*,
        coalesce(
            case upper(ph.channel2)
                when 'DIRECT' then h1.partner_manager_name
                when 'ISO' then ''
                when 'REFERRAL' then h2.partner_manager_name_secondary
                else ''
            end, ''
        ) as merchant_account_manager,
        coalesce(
            case upper(ph.channel2)
                when 'DIRECT' then ''
                when 'ISO' then h1.partner_manager_name
                when 'REFERRAL' then h1.partner_manager_name
                else ''
            end, ''
        ) as partner_account_manager
    from
        existing_or_new_added as t
    left join sales_person as h1
        on
            t.bid = h1.bid
            and t.period_start_date between h1.startrange and h1.endrange
    left join secondary_sales_person as h2
        on
            t.bid = h2.bid
            and t.period_start_date between h2.startrange and h2.endrange
    left join partner_channel as ph
        on
            t.partner_id = ph.partner_id
            and t.period_start_date between ph.startrange and ph.endrange
),

first_turnover_date as (
    select
        businessid,
        min(transactiondate) as first_transaction_date
    from
        {{ ref('stg_mssql__analytics_dbo_tblfinancialreport') }}
    where
        totalturnovergbp > 0
    group by
        businessid
),

is_first_trading_month_added as (
    -- If business has first turnover transaction in the current period, it is it's first trading month.
    select
        t.*,
        case
            when
                trunc(date_trunc('month', ftd.first_transaction_date))
                = t.period_start_date
                then 'Yes'
            else 'No'
        end as is_first_trading_month
    from
        managers_added as t
    left join first_turnover_date as ftd
        on t.bid = ftd.businessid
),

pnl_columns_added as (
    select
        *,
        acq_total_revenue_gbp
        - auto_early_settlement_revenue_gbp as acq_revenue_gbp,
        acq_total_revenue_gbp
        + gateway_revenue_gbp
        + pci_revenue_gbp
        + account_updater_total_revenue_gbp
        + fx_total_revenue_gbp
        + manual_early_settlement_gbp
        + remittance_fee_revenue_gbp
        + manual_adjusted_revenue_gbp
        + "3ds_revenue_gbp" as total_revenue_gbp,
        total_revenue_gbp
        - interchange_fee
        - scheme_fee
        - manual_adjusted_cos_gbp
        - fx_cost_gbp
        - "3ds_cost_gbp" as net_revenue,
        net_revenue
        - partner_commission
        - third_party_gateway_cost
        - remittance_fee_cost_gbp as gross_profit,
        gross_profit
        - value_allocated_scheme_fee
        - value_allocated_interchange as adjusted_gross_profit
    from
        is_first_trading_month_added
),

final as (
    select
        *,
        case bid
            when 999991 then 'ADJ'
            when 999992 then 'ADJ'
            when 999993 then 'ADJ'
            when 999994 then 'ADJ'
            when 999995 then 'ADJ'
            else cast(bid as text)
        end as business_id,
        case bid
            when 999991 then comment
            when 999992 then 'ATM'
            when 999993 then 'Issuing'
            when 999994 then 'ACQ Business Account'
            when 999995 then 'Gateway Fees - Acceptance COS'
            else merchant_name
        end as business_name,
        case bid
            when 999991 then ''
            when 999992 then 'ATM'
            when 999993 then 'Other'
            when 999994 then 'Other'
            when 999995 then 'Other'
            else channel_original
        end as channel
    from
        pnl_columns_added
)

select *
from
    final
{% endmacro %}