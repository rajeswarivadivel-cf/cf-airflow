with

invoices as (
    select
        *,
        case invoice_account
            when '10067784-826-0' then 'CFE'
            when '10079833-528-0' then 'PCO'
        end as cfe_or_pco,
        case cfe_or_pco
            when 'CFE' then 'ACQ'
            when 'PCO' then 'ACQ'
        end as default_product,
        case cfe_or_pco
            when 'CFE' then 'CashFlows'
            when 'PCO' then 'PayCheckout'
        end as default_processor
    from {{ ref('stg_visa__scheme_fees') }}
),

joined as (
    select
        inv.*,
        to_gbp_rate.rate as fx_rate,
        coalesce(inv.total, 0) + coalesce(inv.tax, 0) as combined_amount,
        coalesce(inv.total, 0) * fx_rate as cost_gbp,
        coalesce(inv.tax, 0) * fx_rate as vat_gbp,
        coalesce(b.bin, inv.entity_id) as bin,
        coalesce(b.product_accounting, inv.default_product) as reporting_product,
        coalesce(b.processor_accounting, inv.default_processor) as processor,
        coalesce(b.iad_accounting, inv.default_processor) as iad,
        d.new_fee_guide_type as fee_guide_lookup,
        d.new_section_name_level_1 as event_group,
        coalesce(
            b.product_actual, inv.default_product
        ) as product_related_from_scheme,
        d.recovery as recovery_position,
        d.comment_or_actions as actions_required,
        d.billing_cycle as frequency,
        '' as comments,
        coalesce(reporting_product, inv.default_product) || ' - ' || case
            when lower(inv.current_or_previous) = 'current billing'
                then 'Next Collection - '
            else 'Previously Collected - '
        end
        || coalesce(
            b.settlement_entity, inv.default_processor
        ) as reporting_summary,
        case lower(inv.current_or_previous)
            when
                'previously collected'
                then
                    'Previous Collected from '
                    || coalesce(b.settlement_entity, inv.default_processor)
            when
                'current billing'
                then
                    'To be collected from '
                    || coalesce(b.settlement_entity, inv.default_processor)
            else 'Check'
        end as settlement_status
    from invoices as inv
    left join {{ ref('bin_map') }} as b
        on
            inv.invoice_account = b.invoice_account
            and coalesce(inv.mapping, inv.entity_id) = b.bin
    left join {{ ref('detail_billing_ids') }} as d
        on inv.billing_line = d.event_id
    left join {{ ref('stg_mssql__accounts_dbo_fx_rates') }} as to_gbp_rate
        on
            inv.invoice_date = date(to_gbp_rate.rate_date)
            and lower(inv.billing_currency) = lower(to_gbp_rate.from_currency)
            and lower(to_gbp_rate.to_currency) = 'gbp'

)

select *
from joined
