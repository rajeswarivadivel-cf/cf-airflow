with source as (

    select *
    from {{ source('analytics', 'tbldatalake_editcriteria_and_charge') }}

),

renamed as (

    select *

    from source

)

select * from renamed
