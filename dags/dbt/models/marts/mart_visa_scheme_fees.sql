with
source as (
    select *
    from
        {{ ref('stg_visa__scheme_fees') }}
)

select *
from
    source
