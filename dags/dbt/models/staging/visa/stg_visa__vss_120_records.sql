with source as (

    select * from {{ source('analytics', 'visa__vss_120_records') }}

),

renamed as (

    select *

    from source

)

select * from renamed
