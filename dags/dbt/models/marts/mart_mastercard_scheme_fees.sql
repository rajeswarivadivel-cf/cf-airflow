with
source as (
    select *
    from
        {{ ref('stg_mastercard__scheme_fees') }}
)

select *
from
    source
