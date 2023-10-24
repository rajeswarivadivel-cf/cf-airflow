with source as (

    select * from {{ source('analytics', 'visa__vss_110_records') }}

),

renamed as (

    select *

    from source

)

select * from renamed
