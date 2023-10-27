{% macro mart_partner_managers_by_bid_base(old_table, column) %}
with old_source as (
    select
        bid::varchar,
        {{ column }},
        startdate as valid_from,
        enddate as valid_to
    from {{ old_table }}
),

new_source as (
    select
        bid,
        {{ column }},
        valid_from,
        valid_to
    from {{ ref('int_salesperson_history') }}
),

combined as (
    select * from old_source
    union distinct
    select * from new_source
),

sequence_added as (
    select
        *,
        row_number()
            over (partition by bid order by valid_from, valid_to)
        as sequence
    from combined
    order by bid, valid_from, valid_to
),

valid_to_updated as (
    select
        curr.bid,
        curr.sequence,
        curr.{{ column }},
        curr.valid_from,
        coalesce(
            curr.valid_to, next.valid_from - interval '1 day'
        )::date as valid_to
    from sequence_added as curr
    left join
        sequence_added as next
        on curr.bid = next.bid and curr.sequence = next.sequence - 1
),

ordered as (
    select * from valid_to_updated order by bid, sequence
)

select * from ordered
{% endmacro %}