with

transactions as (
    select *
    from
        {{ ref('stg_mssql__analytics_dbo_tbldatalake') }}
    where
        lower(scheme) = 'mastercard'
        and lower(transactiontype) in ('sale', 'refund', 'sale with cashback')
        and lower(transactionstatus) in ('commissioned', 'paid', 'reserved')
),

sales as (
    select *
    from {{ ref('stg_mssql__analytics_dbo_tbldatalake') }}
    where lower(transactiontype) = 'sale'
),

refunds as (
    select *
    from {{ ref('stg_mssql__analytics_dbo_tbldatalake') }}
    where lower(transactiontype) = 'refund'
),

-- Find the corresponding sale transaction ARN for each refund transaction ARN.
refund_to_sale_arn_map as (
    select
        r.arn as refund_arn,
        s.arn as sale_arn,
        row_number()
            over (partition by r.arn order by s.postingtime desc)
        as rank
    from
        refunds as r
    left join
        sales as s
        on r.tran_first_ref = s.tran_first_ref
),

-- Force the relation to becomes a 1-to-1 relation.
refund_to_latest_sale_arn_map as (
    select
        refund_arn,
        sale_arn
    from refund_to_sale_arn_map
    where rank = 1
),

currency_codes as (
    select
        cast(numeric_code as int) as numeric_code,
        alphabetic_code
    from {{ ref('currency_codes') }}
),

mastercard_transactions as (
    select
        mc.*,

        /*
        In Mastercard data, for refund transactions, most ARNs provided is not
        the refund transaction ARNs but their corresponding sales transaction
        ARNs. Here we add the "correct" refund transaction ARNs based on
        datalake data.
        */
        case mc.feeprocessingcode
            when 19 then mc.acquirerreferencedata --Sale

            /*
            Check if the ARN provided by Mastercard belongs to a sale or refund
            transaction and perform accordingly.
            */
            when 29 then (
                case lower(dl.transactiontype)
                    when 'sale' then map.refund_arn
                    when 'refund' then mc.acquirerreferencedata
                end
            ) --Refund

        end as correct_arn,

        /*
        In Mastercard data, all amounts are positive, while in datalake refund
        transactions' interchange amounts are negative. Add signage for later
        conversion to handle this misalignment.
        */
        case mc.feeprocessingcode
            when 19 then 1 --Sale
            when 29 then -1 --Refund
        end as interchange_signage

    from
        {{ ref('stg_mastercard__clearing_report_ip755120_aa') }} as mc
    left join
        currency_codes as curr
        on mc.currencycode = curr.numeric_code
    left join
        refund_to_latest_sale_arn_map as map
        on mc.acquirerreferencedata = map.sale_arn
    left join
        transactions as dl
        on mc.acquirerreferencedata = dl.arn
),

joined as (
    select
        t.datalakeid as datalake_id,
        t.transactiontype as transaction_type,
        t.transactionstatus as transaction_status,
        cast(t.postingtime as date) as transaction_posted_on,
        t.arn,
        t.transactionid as transaction_id,
        t.mcc,
        t.regionality,
        t.merchantcountry as merchant_country,
        t.transactioncurrency as transaction_currency,
        dl_supp.editcriteria_productid as product_id,
        dl_supp.editcriteria_afs as afs,
        sar.intchg_rate_tier as interchange_rate_tier,
        mc.traceid as mastercard_trace_id,
        mc.acquirerreferencedata as mastercard_arn,
        mc.processingcode / 10000 as mastercard_processing_code,
        mc.functioncode as mastercard_function_code,
        mc.acceptorbusinesscode as mastercard_acceptor_business_code,
        mc.acceptorcountrycode as mastercard_acceptor_country_code,
        mc.businessservicearrangementtypecode as mastercard_business_service_arrangement_type_code,
        mc.businessserviceidcode as mastercard_business_service_code,
        mc.interchangeratedesignator as mastercard_interchange_rate_designator,
        mc.globalclearing_pi as masterecard_global_clearing_pi,
        mc.licensed_pi as mastercard_licensed_pi,
        mc.cardprogramidentifier as mastercard_card_program_identifier,
        mc.centralsitebusinessdate as mastercard_central_site_business_date,
        mc.settlementdate as mastercard_settlement_date,
        cc.alphabetic_code as mastercard_currency,
        -- Amount fields.
        t.transactionamount as transaction_amount,
        t.transactionamountgbp as transaction_amount_gbp,
        t.interchange as interchange_amount,
        t.interchangegbp as interchange_amount_gbp,
        cast(mc.amount as numeric) / 100 as mastercard_transaction_amount,
        (
            case
                when mc.e_feesettlementindicator = 1
                    then cast(mc.e_interchangeamountfee as numeric) / 1000000
                else cast(mc.transactionfee as numeric) / 100
            end
        )
        * mc.interchange_signage as mastercard_interchange_amount_including_edp,
        cast(mc.transactionfee as numeric)
        / 100
        * mc.interchange_signage as mastercard_interchange_amount_excluding_edp
    from
        transactions as t
    left join
        {{ ref('stg_tbldatalake_editcriteria_and_charge') }} as dl_supp
        on t.transactionid = dl_supp.transactionid
    left join {{ ref('stg_mssql__core_dbo_sales') }} as s
        on t.transactionid = s.reference
    left join
        {{ ref('stg_mssql__core_dbo_sale_auth_responses') }} as sar
        on s.id = sar.id
    left join
        mastercard_transactions as mc
        on t.arn = mc.correct_arn
    left join
        currency_codes as cc
        on mc.currencycode = cc.numeric_code
),

edp_column_added as (
    select
        *,
        mastercard_interchange_amount_including_edp
        - mastercard_interchange_amount_excluding_edp as edp_amount
    from joined
),

exchange_rates_added as (
    select
        *,
		{{ dbt_utils.safe_divide(
			'transaction_amount_gbp',
			'transaction_amount'
		) }} as fx_rate
    from edp_column_added
),

gbp_columns_added as (
    select
        *,
        mastercard_transaction_amount
        * fx_rate as mastercard_transaction_amount_gbp,
        mastercard_interchange_amount_including_edp
        * fx_rate as mastercard_interchange_amount_including_edp_gbp,
        mastercard_interchange_amount_excluding_edp
        * fx_rate as mastercard_interchange_amount_excluding_edp_gbp,
        edp_amount * fx_rate as edp_amount_gbp
    from exchange_rates_added
),

days_columns_added as (
    select
        *,
        mastercard_central_site_business_date
        - transaction_posted_on as days_from_transaction_posted_on_to_mastercard_central_site_business_date,
        mastercard_settlement_date
        - transaction_posted_on as days_from_transaction_posted_on_to_mastercard_settlement_date
    from gbp_columns_added
),

reasons_added as (
    select
        *,
        case
            when
                mastercard_transaction_amount is null
                or mastercard_interchange_amount_excluding_edp is null
                then 'NOT_FOUND'
            when
                transaction_currency != mastercard_currency
                then 'CURRENCY_NOT_MATCHED'
            when
                abs(
                    transaction_amount - mastercard_transaction_amount
                )
                < 1
                and abs(
                    interchange_amount
                    - mastercard_interchange_amount_excluding_edp
                )
                < 0.01
                then 'ROUNDING'
            else 'OTHERS'
        end as reason
    from days_columns_added
)

select *
from
    reasons_added
