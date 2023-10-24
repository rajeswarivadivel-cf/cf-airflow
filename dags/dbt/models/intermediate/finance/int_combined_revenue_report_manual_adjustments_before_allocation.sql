with
source as (
    select *
    from
        {{ ref('stg_crr_manual_adjustments') }}
),

period_created_ats as (
    select distinct
        period,
        created_at
    from
        source
),

ranks_added as (
    select
        *,
        row_number() over (partition by period order by created_at desc) as rank
    from
        period_created_ats
),

latests as (
    select source.*
    from
        ranks_added as t
    left join
        source
        on t.period = source.period and t.created_at = source.created_at
    where
        t.rank = 1
),

ordered as (
    select *
    from
        latests
    order by
        period,
        type,
        bid
)

select *
from
    ordered
