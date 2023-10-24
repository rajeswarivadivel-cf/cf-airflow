with source as (

    select *
    from
        {{ source('analytics', 'crr_non_sales_scheme_fees_by_period_by_merchant') }}

),

renamed as (

    select *

    from source

)

select * from renamed
