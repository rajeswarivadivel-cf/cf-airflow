/*This aims to replace analytics.core.sp_maintain_bidbysalesperson_history which generates history of sales person.
Noted that since historical snapshots are not available, this is not a complete history.*/
with source as (
    select
        id,
        loaded_at,
        bid,
        partner_manager_name,
        partner_manager_name_secondary,
        row_number()
            over (partition by bid, loaded_at::date order by loaded_at desc)
        as rank
    from {{ ref('stg_mssql__analytics_dbo_tblbidbysalesperson_snapshots') }}
),

latest_of_each_day as (
    select
        id,
        loaded_at::date as loaded_on,
        bid,
        partner_manager_name,
        partner_manager_name_secondary,
        row_number() over (partition by bid order by loaded_on) as sequence
    from source
    where rank = 1
),

is_changed_added as (
    select
        curr.*,
        coalesce(
            curr.partner_manager_name != prev.partner_manager_name
            or curr.partner_manager_name_secondary
            != prev.partner_manager_name_secondary,
            true
        ) as is_changed
    from latest_of_each_day as curr
    left join latest_of_each_day as prev
        on
            curr.bid = prev.bid
            and curr.sequence = prev.sequence + 1
),

changed_only as (
    select
        id,
        loaded_on,
        bid,
        partner_manager_name,
        partner_manager_name_secondary,
        row_number() over (partition by bid order by loaded_on) as sequence
    from is_changed_added
    where is_changed = true
),

valid_period_added as (
    select
        curr.*,
        curr.loaded_on as valid_from,
        (next.loaded_on - interval '1 day')::date as valid_to
    from changed_only as curr
    left join changed_only as next
        on
            curr.bid = next.bid
            and curr.sequence = next.sequence - 1
),

final as (
    select
        bid,
        sequence,
        partner_manager_name,
        partner_manager_name_secondary,
        valid_from,
        valid_to
    from valid_period_added
    order by bid, sequence
)

select * from final
