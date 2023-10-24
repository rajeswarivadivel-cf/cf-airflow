with source as (

    select *
    from {{ source('analytics', 'mssql__latest_record_counts') }}

),

renamed as (

    select *

    from source

)

select * from renamed
