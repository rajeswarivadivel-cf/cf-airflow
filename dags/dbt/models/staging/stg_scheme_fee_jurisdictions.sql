with source as (

    select *
    from {{ source('analytics', 'scheme_fee_jurisdictions') }}

),

renamed as (

    select *

    from source

)

select * from renamed
