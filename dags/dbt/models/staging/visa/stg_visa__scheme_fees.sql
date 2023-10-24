with source as (

    select * from {{ source('analytics', 'visa__scheme_fees') }}

),

renamed as (

    select *

    from source

)

select * from renamed
