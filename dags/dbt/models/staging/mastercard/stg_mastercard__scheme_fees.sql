with source as (

    select * from {{ source('analytics', 'mastercard__scheme_fees') }}

),

renamed as (

    select *

    from source

)

select * from renamed
