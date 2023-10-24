with source as (

    select * from {{ source('analytics', 'crr_manual_adjustments') }}

),

renamed as (

    select *

    from source

)

select * from renamed
