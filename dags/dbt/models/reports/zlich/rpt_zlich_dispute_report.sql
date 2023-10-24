with
source as (
    select
        *,
        datediff(
            day, pre_arbitration_created_at, reversal_created_at
        ) as pre_arbitration_to_reversal_days,
        datediff(
            day, dispute_created_at, reversal_created_at
        ) as dispute_to_reversal_days,
        datediff(day, dispute_created_at, getdate()) as dispute_to_now_days,
        case
            when (
                pre_arbitration_description is not null
                and pre_arbitration_to_reversal_days <= 30
            )
            or reversal_description is not null then 'Won'
            when
                dispute_to_reversal_days is null and dispute_to_now_days > 60
                then 'Lost'
            when
                dispute_to_reversal_days is null and dispute_to_now_days <= 60
                then 'Pending'
        end as status
    from
		{{ ref('int_disputes') }}
    where
        mid = 5950347
    order by
        dispute_created_at
),

selected as (
    select
        dispute_reference,
        sale_description,
        sale_amount,
        sale_created_at,
        dispute_description,
        dispute_created_at,
        response_description,
        response_created_at,
        reversal_description,
        reversal_created_at,
        pre_arbitration_description,
        pre_arbitration_created_at,
        pre_arbitration_to_reversal_days,
        dispute_to_reversal_days,
        dispute_to_now_days,
        status
    from
        source
)

select *
from
    selected
