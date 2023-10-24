with source as (

    select *
    from {{ source('analytics', 'mssql_table_record_counts') }}

),

renamed as (

    select *

    from source

)

select * from renamed
