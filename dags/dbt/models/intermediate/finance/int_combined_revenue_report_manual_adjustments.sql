with
source as (
    select *
    from
        {{ ref('int_combined_revenue_report_manual_adjustments_before_allocation') }}
    where
        coalesce(revenue, 0) != 0
        or coalesce(cos, 0) != 0
),

grouped as (
    select
        date_part(year, period) as year,
        date_part(month, period) as month,
        bid,
        0 as acqtotalrevenuegbp,
        sum(
            case when lower(type) = 'gateway' then revenue else 0 end
        ) as gatewayrevenuegbp,
        sum(
            case when lower(type) = 'pci' then revenue else 0 end
        ) as pcirevenuegbp,
        sum(
            case when lower(type) = 'account_updater' then revenue else 0 end
        ) as accountupdatertotalrevenuegbp,
        sum(
            case when lower(type) = 'fx' then revenue else 0 end
        ) as fxtotalrevenuegbp,
        sum(
            case
                when lower(type) = 'manual_early_settlement' then revenue else 0
            end
        ) as manualearlysettlementgbp,
        0 as autoearlysettlementrevenuegbp,
        sum(
            case when lower(type) = 'partner_commission' then revenue else 0 end
        ) as totalpartnercommissiongbp,
        sum(
            case
                when lower(type) = 'interchange_adjustment' then revenue else 0
            end
        ) as ic_costs,
        sum(
            case
                when lower(type) = 'scheme_fee_adjustment' then revenue else 0
            end
        ) as sf_costs,
        sum(
            case when lower(type) = 'manual_adjustment' then revenue else 0 end
        ) as revenuegbp,
        sum(case
            when
                lower(type) in (
                    'third_party_gateway_cost',
                    'third_party_gateway_cost_adjustment'
                )
                then cos
            else 0
        end) as thirdpartygatewaycosts,
        sum(
            case when lower(type) = 'manual_adjustment' then cos else 0 end
        ) as cosgbp,
        sum(
            case when lower(type) = 'sales_commission' then revenue else 0 end
        ) as salescommissions,
        comment as comments,
        0 as totalturnovergbp,
        sum(case when lower(type) = 'fx' then cos else 0 end) as totalcosgbp,
        sum(
            case when lower(type) = 'remittance' then revenue else 0 end
        ) as remittancefeerevenuegbp,
        sum(
            case when lower(type) = 'remittance' then cos else 0 end
        ) as remittancefeecostgbp,
        0 as transactioncount,
        sum(case
            when lower(type) = 'value_allocated_scheme_fee' then cos
            else 0
        end) as value_allocated_scheme_fees,
        sum(case
            when lower(type) = 'value_allocated_interchange'
                then cos
            else 0
        end) as value_allocated_interchange,
        sum(
            case when lower(type) = '3ds' then cos else 0 end
        ) as "3ds cost gbp",
        sum(
            case when lower(type) = '3ds' then revenue else 0 end
        ) as "3ds revenue gbp"
    from
        source
    group by
        date_part(year, period),
        date_part(month, period),
        bid,
        comment
)

select *
from
    grouped
