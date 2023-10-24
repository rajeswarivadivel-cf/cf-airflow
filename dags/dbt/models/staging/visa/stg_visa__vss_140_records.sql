with source as (

    select * from {{ source('analytics', 'visa__vss_140_records') }}

),

renamed as (

    select *

    from source

)

select * from renamed
