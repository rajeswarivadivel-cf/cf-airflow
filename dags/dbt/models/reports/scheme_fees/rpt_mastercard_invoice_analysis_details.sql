with

invoices as (
    select *
    from analytics.mastercard__scheme_fees
),

joined as (
    select
        inv.*,
        to_gbp_rate.rate as fx_rate,
        coalesce(total_charge * fx_rate, 0) as service_gbp,
        coalesce(vat_charge * fx_rate, 0) as vat_gbp,
        service_gbp + vat_gbp as combined_total_gbp,
        i.product as product,
        i.processor as processor,
        i.program as program,
        d.billing_cycle as billing_cycle,
        d.recovery as recovery_position,
        d.comment_or_actions as comments,
        i.product
        || ' - '
        || inv.collection_method
        || ' - '
        || i.processor as reporting_summary,
        case lower(inv.collection_method)
            when 'gcms' then 'Settlement to '
            else 'Manual payment by Cashflows'
        end
        || case lower(i.processor)
            when 'cashflows' then 'Cashflows'
            else 'Others'
        end as settlement_summary
    from invoices as inv
    left join analytics.mastercard_ica_map as i
        on inv.invoice_ica = i.ica
    left join analytics.mastercard_detail_billing_map as d
        on inv.event_id = d.line_id_event_id
    left join analytics.mssql__accounts_dbo_fx_rates as to_gbp_rate
        on
            inv.billing_cycle_date = date(to_gbp_rate.rate_date)
            and lower(inv.currency) = lower(to_gbp_rate.from_currency)
            and lower(to_gbp_rate.to_currency) = 'gbp'
)

select *
from joined
