with
source as (
    select
        *,
        datediff(
            day, representment_created_at, second_chargeback_created_at
        ) as representment_to_second_chargeback_days,
        datediff(
            day, second_chargeback_created_at,
            coalesce(reversal_created_at, second_chargeback_created_at)
        ) as second_chargeback_to_reversal_days,
        datediff(
            day, second_chargeback_created_at, getdate()
        ) as second_chargeback_to_now_days,
        datediff(
            day, representment_created_at, getdate()
        ) as representment_to_now_days,
        datediff(
            day, chargeback_created_at, getdate()
        ) as chargeback_to_now_days,
        case
            when (
                representment_to_second_chargeback_days between 1 and 45
                and second_chargeback_to_reversal_days between 1 and 30
            )
            or (
                representment_to_second_chargeback_days is null
                and representment_to_now_days > 45
            ) then 'Won'
            when (
                representment_to_second_chargeback_days >= 1
                and second_chargeback_to_reversal_days = 0
                and second_chargeback_to_now_days > 30
            )
            or (
                representment_description is null
                and chargeback_to_now_days > 45
            )
                then 'Lost'
            when (
                representment_to_second_chargeback_days between 1 and 45
                and second_chargeback_to_reversal_days = 0
                and second_chargeback_to_now_days < 30
            )
            or (
                representment_to_second_chargeback_days is null
                and representment_to_now_days <= 45
            )
            or (
                representment_description is null
                and chargeback_to_now_days <= 45
            ) then 'Pending'
        end as status
    from
        {{ ref('int_chargebacks') }}
    where
        mid = 5950347
        and sale_description is not null
    order by sale_created_at
),

renamed as (
    select
        chargeback_reference as charegebackref,
        sale_description as description,
        sale_amount as saleamount,
        sale_created_at as saledatetime,
        chargeback_description as chargeback,
        chargeback_created_at as chargebacktime,
        representment_description as representment,
        representment_created_at as representmenttime,
        second_chargeback_description as secondchargeback,
        second_chargeback_created_at as secondchargebacktime,
        reversal_description as chargebackreversal,
        reversal_created_at as chargebackreversaltime,
        representment_to_second_chargeback_days as representmenttosecondchargedays,
        second_chargeback_to_reversal_days as secondchargetoreversaldays,
        representment_to_now_days as representationwaitperiod,
        second_chargeback_to_now_days as reversalwaitperiod,
        status as status
    from
        source
)

select *
from
    renamed
