with

base_0 as (
    select
        s.status,
        s.store_id,
        m.merchant_id,
        m.merchant_type,
        ta.acquirer_id,
        ta.terminal_id,
        ta.terminal_status,
        ta.trans_class_id,
        case ta.acquirer_id
            when 17 then 'PayCheckout'
            else 'Cashflows'
        end as report_type
    from analytics.mssql__core_dbo_stores as s
    inner join
        analytics.mssql__core_dbo_merchants as m
        on s.merchant_id = m.merchant_id
    left join
        analytics.mssql__core_dbo_terminal_acquirers as ta
        on s.store_id = ta.store_id
    where
        s.status = 2
        and ta.acquirer_id in (9, 17, 10)
),

base_1 as (
    select *
    from base_0
    where
        merchant_type = 6
        and terminal_status = 1
        and trans_class_id = -2
),

base_2 as (
    select
        b.*,
        a.acquirer_name
    from base_0 as b
    inner join
        analytics.mssql__core_dbo_acquirers as a
        on b.acquirer_id = a.acquirer_id
    where
        b.merchant_type = 6
        and b.terminal_status = 1
),

base_3 as (
    select *
    from base_0
    where
        merchant_type = 15
),

total_number_of_merchants as (
    select distinct
        'Total Number of Merchants' as metric_type,
        count(distinct merchant_id) as acceptance_information,
        report_type
    from base_2
    group by
        report_type,
        acquirer_name
),

total_number_of_merchant_outlets as (
    select distinct
        'Total Number of Merchant Outlets' as metric_type,
        count(distinct store_id) as acceptance_information,
        report_type
    from base_2
    group by
        report_type,
        acquirer_name
),

total_number_of_merchant_terminals as (
    select distinct
        'Total Number of Merchant Terminals' as metric_type,
        count(distinct terminal_id) as acceptance_information,
        report_type
    from base_2
    group by report_type
),

total_number_of_merchant_terminals_traditional_point_of_sales as (
    select distinct
        'Total Number of Merchant Terminals - Traditional Point of Sales' as metric_type,
        count(distinct terminal_id) as acceptance_information,
        report_type
    from base_2
    group by report_type
),

number_of_merchant_terminals_chip_enabled as (
    select distinct
        'Number of Merchant Terminals - Chip enabled' as metric_type,
        count(distinct terminal_id) as acceptance_information,
        report_type
    from base_1
    group by report_type
),

number_of_merchant_terminals_contactless_enabled as (
    select distinct
        'Number of Merchant Terminals - Contactless enabled' as metric_type,
        count(distinct terminal_id) as acceptance_information,
        report_type
    from base_1
    group by report_type
),

number_of_merchant_terminals_pin_entry_enabled as (
    select distinct
        'Number of Merchant Terminals - Pin Entry enabled' as metric_type,
        count(distinct terminal_id) as acceptance_information,
        report_type
    from base_1
    group by report_type
),

number_of_merchant_terminals_magnetic_stripe_enabled as (
    select distinct
        'Number of Merchant Terminals - Magnetic Stripe enabled' as metric_type,
        count(distinct terminal_id) as acceptance_information,
        report_type
    from base_1
    group by report_type
),

total_number_of_registered_payment_facilitators as (
    select distinct
        'Total Number of Registered Payment Facilitators' as metric_type,
        count(distinct merchant_id) as acceptance_information,
        report_type
    from base_3
    group by report_type
),

number_of_merchant_outlets_sponsored_by_payment_facilitators as (
    select distinct
        'Number of Merchant Outlets Sponsored by Payment Facilitators' as metric_type,
        count(distinct store_id) as acceptance_information,
        report_type
    from base_3
    group by report_type
),

combined as (
{% for cte in [
    'total_number_of_merchants',
    'total_number_of_merchant_outlets',
    'total_number_of_merchant_terminals',
    'total_number_of_merchant_terminals_traditional_point_of_sales',
    'number_of_merchant_terminals_chip_enabled',
    'number_of_merchant_terminals_contactless_enabled',
    'number_of_merchant_terminals_pin_entry_enabled',
    'number_of_merchant_terminals_magnetic_stripe_enabled',
    'total_number_of_registered_payment_facilitators',
    'number_of_merchant_outlets_sponsored_by_payment_facilitators'
] %}
    select *, {{ loop.index }} as "order" from {{ cte }}
    {% if not loop.last %}
        union all
    {% endif %}
{% endfor %}
),

unpivoted as (
    select
        'Visa' as product,
        "order" || '. ' || metric_type as name,
        sum(
            case report_type
                when 'Cashflows' then acceptance_information
            end
        ) as cfe,
        sum(
            case report_type
                when 'PayCheckout' then acceptance_information
            end
        ) as pco
    from
        combined
    group by
        "order",
        metric_type
    order by
        "order"
)

select *
from unpivoted
