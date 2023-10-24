{# vw_BID_MCC updates every second. #}

{% set old_query %}
select
    cast(periodstamp as date) as period_start_date,
    businessid as business_id,
    businessname as business_name,
    channel as channel,
    channel2 as channel_2,
    partner_name as partner_name,
    "existing/new" as existing_or_new,
    comments as comment,
    "merchant account manager" as merchant_account_manager,
    "partner account manager" as partner_account_manager,
    --cast(mcc as integer) as mcc,
    --description as description,
    firsttransactiondate as first_transaction_date,
    merchants_first_trading_month as is_first_trading_month,
    legal_entity as legal_entity,
    transactioncount as transaction_count,
    floor(purchaseturnovergbp) as purchase_turnover_gbp,
    floor(acqrevenuegbp) as acq_revenue_gbp,
    floor(gatewayrevenuegbp) as gateway_revenue_gbp,
    floor(gatewaypcirevenuegbp) as gateway_pci_revenue_gbp,
    floor(accountupdaterrevenuegbp) as account_updater_revenue_gbp,
    floor(fxrevenuegbp) as fx_revenue_gbp,
    floor(manearlysettlementrevenuegbp) as manual_early_settlement_revenue_gbp,
    floor(autoearlysettlementrevenuegbp) as auto_early_settlement_revenue_gbp,
    floor("3ds revenue gbp") as "3ds_revenue_gbp",
    floor(remittancefeerevenuegbp) as remittance_fee_revenue_gbp,
    floor(manualadjrevenuegbp) as manual_adjusted_revenue_gbp,
    floor(totalrevenuegbp) as total_revenue_gbp,
    floor(interchange) as interchange_fee,
    floor(scheme_fees) as scheme_fee,
    floor(manualadjcosgbp) as manual_adjusted_cos_gbp,
    floor(fxcostgbp) as fx_cost_gbp,
    floor(net_revenue) as net_revenue,
    floor(remittancefeecostgbp) as remittance_fee_cost_gbp,
    floor(partner_commission) as partner_commission,
    floor(thirdpartygatewaycosts) as third_party_gateway_cost,
    floor("3ds cost gbp") as "3ds_cost_gbp",
    floor("gross profit") as gross_profit,
    floor(value_allocated_interchange) as value_allocated_interchange,
    floor(value_allocated_scheme_fees) as value_allocated_scheme_fee,
    floor("adjusted gp") as adjusted_gross_profit
from
    {{ ref('stg_mssql__analytics_dbo_vw_combinedrevenuereport') }}
where
    cast(periodstamp as date) = {{ previous_period_start_date() }}
{% endset %}

{% set new_query %}
select
    period_start_date,
    business_id,
    business_name,
    channel,
    channel_2,
    partner_name,
    existing_or_new,
    comment,
    merchant_account_manager,
    partner_account_manager,
    --mcc,
    --description,
    first_transaction_date,
    is_first_trading_month,
    legal_entity,
    transaction_count,
    floor(purchase_turnover_gbp) as purchase_turnover_gbp,
    floor(acq_revenue_gbp) as acq_revenue_gbp,
    floor(gateway_revenue_gbp) as gateway_revenue_gbp,
    floor(gateway_pci_revenue_gbp) as gateway_pci_revenue_gbp,
    floor(account_updater_revenue_gbp) as account_updater_revenue_gbp,
    floor(fx_revenue_gbp) as fx_revenue_gbp,
    floor(
        manual_early_settlement_revenue_gbp
    ) as manual_early_settlement_revenue_gbp,
    floor(
        auto_early_settlement_revenue_gbp
    ) as auto_early_settlement_revenue_gbp,
    floor("3ds_revenue_gbp") as "3ds_revenue_gbp",
    floor(remittance_fee_revenue_gbp) as remittance_fee_revenue_gbp,
    floor(manual_adjusted_revenue_gbp) as manual_adjusted_revenue_gbp,
    floor(total_revenue_gbp) as total_revenue_gbp,
    floor(interchange_fee) as interchange_fee,
    floor(scheme_fee) as scheme_fee,
    floor(manual_adjusted_cos_gbp) as manual_adjusted_cos_gbp,
    floor(fx_cost_gbp) as fx_cost_gbp,
    floor(net_revenue) as net_revenue,
    floor(remittance_fee_cost_gbp) as remittance_fee_cost_gbp,
    floor(partner_commission) as partner_commission,
    floor(third_party_gateway_cost) as third_party_gateway_cost,
    floor("3ds_cost_gbp") as "3ds_cost_gbp",
    floor(gross_profit) as gross_profit,
    floor(value_allocated_interchange) as value_allocated_interchange,
    floor(value_allocated_scheme_fee) as value_allocated_scheme_fee,
    floor(adjusted_gross_profit) as adjusted_gross_profit
from
    {{ ref('rpt_finance_combined_revenue_report') }}
where
    period_start_date = {{ previous_period_start_date() }}
{% endset %}

{{ audit_helper.compare_queries (
    a_query=old_query,
    b_query=new_query,
    primary_key='period_start_date || business_id || business_name || comment || existing_or_new || merchant_account_manager || partner_account_manager || channel || channel_2 || partner_name || first_transaction_date || is_first_trading_month || legal_entity',
    summarize=False
) }}
