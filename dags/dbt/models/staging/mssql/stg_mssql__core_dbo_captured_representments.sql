with source as (

    select *
    from {{ source('analytics', 'mssql__core_dbo_captured_representments') }}

),

renamed as (

    select *

    from source

)

select * from renamed
