with
source as (
    select
        *,
        datediff(
            day, response_created_at, pre_arbitration_created_at
        ) as response_to_pre_arbitration_days,
        datediff(
            day, response_created_at, reversal_created_at
        ) as response_to_reversal_days,
        datediff(
            day, pre_arbitration_created_at, reversal_created_at
        ) as pre_arbitration_to_reversal_days,
        datediff(day, response_created_at, getdate()) as response_to_now_days,
        datediff(
            day, pre_arbitration_created_at, getdate()
        ) as pre_arbitration_to_now_days,
        datediff(day, dispute_created_at, getdate()) as dispute_to_now_days,
        case
            when (
                response_to_now_days > 30
                and response_to_pre_arbitration_days is null
                and response_to_reversal_days is null
            )
            or reversal_description is not null then 'Won'
            when (
                response_to_pre_arbitration_days is not null
                and pre_arbitration_to_reversal_days is null
                and pre_arbitration_to_now_days > 30
            )
            or (
                dispute_to_now_days > 30
                and response_description is null
                and reversal_description is null
                and pre_arbitration_description is null
            ) then 'Lost'
            when (
                response_to_pre_arbitration_days is not null
                and pre_arbitration_to_now_days <= 30
            )
            or (
                dispute_to_now_days <= 30
                and response_description is null
                and reversal_description is null
                and pre_arbitration_description is null
            )
            or (
                response_to_now_days <= 30
                and reversal_description is null
                and pre_arbitration_description is null
            )
                then 'Pending'
        end as status
    from
        {{ ref('int_disputes') }}
    where
        mid = 5950347
    order by
        dispute_created_at
)

select *
from
    source
