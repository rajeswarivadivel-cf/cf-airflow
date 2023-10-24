{% set source = 'int_mastercard_interchange_reconciliation_details' %}
{% set headers = [
	'reason',
	'transaction_posted_on',
	'transaction_type',
	'mastercard_processing_code',
	'transaction_currency',
	'mastercard_currency',
	'interchange_rate_tier',
	'mastercard_interchange_rate_designator',
	'product_id',
	'afs',
	'masterecard_global_clearing_pi',
	'mastercard_licensed_pi',
	'mastercard_card_program_identifier',
	'regionality',
	'mastercard_business_service_arrangement_type_code',
	'mastercard_business_service_code',
	'mcc',
	'mastercard_acceptor_business_code',
	'merchant_country',
	'mastercard_acceptor_country_code',
	'days_from_transaction_posted_on_to_mastercard_central_site_business_date',
	'days_from_transaction_posted_on_to_mastercard_settlement_date',
	'mastercard_function_code'
] %}

with

grouped as (
    select
        {{ headers|join(', ') }},
        coalesce(count(transaction_amount), 0) as count,
        coalesce(count(mastercard_transaction_amount), 0) as mastercard_count,
        coalesce(sum(transaction_amount_gbp), 0) as transaction_amount_gbp,
        coalesce(
            sum(mastercard_transaction_amount_gbp), 0
        ) as mastercard_transaction_amount_gbp,
        coalesce(sum(interchange_amount_gbp), 0) as interchange_amount_gbp,
        coalesce(
            sum(mastercard_interchange_amount_including_edp_gbp), 0
        ) as mastercard_interchange_amount_including_edp_gbp,
        coalesce(
            sum(mastercard_interchange_amount_excluding_edp_gbp), 0
        ) as mastercard_interchange_amount_excluding_edp_gbp,
        coalesce(sum(edp_amount_gbp), 0) as edp_amount_gbp
    from {{ ref(source) }}
    group by
        {{ headers|join(', ') }}
    order by
    {% for header in headers %}
        {{ header }}{% if header == 'transaction_posted_on' %}
            desc
        {% endif %}{% if not loop.last %}

            ,
        {% endif %}
    {% endfor %}
),

internal_2dp_columns_added as (
    select
        *,
        interchange_amount_gbp
        - mastercard_interchange_amount_excluding_edp_gbp as delta_internal_2dp_2dp_actual_gbp,
        interchange_amount_gbp
        - mastercard_interchange_amount_including_edp_gbp as delta_internal_2dp_edp_actual_gbp
    from grouped
),

variance_columns_added as (
    select
        *,
        case
            when mastercard_count = 0 then 1 else {{ dbt_utils.safe_subtract([
            'count',
            'mastercard_count'
            ]) }}
        end as count_variance,
        case
            when mastercard_transaction_amount_gbp = 0 then 1
            else {{ dbt_utils.safe_subtract([
            'transaction_amount_gbp',
            'mastercard_transaction_amount_gbp'
            ]) }}
        end as transaction_amount_variance,
        case
            when mastercard_interchange_amount_excluding_edp_gbp = 0 then 1
            else {{ dbt_utils.safe_subtract([
            'interchange_amount_gbp',
            'mastercard_interchange_amount_excluding_edp_gbp'
            ]) }}
        end as interchange_amount_variance
    from internal_2dp_columns_added
)

select * from variance_columns_added
