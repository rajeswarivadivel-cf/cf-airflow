with source as (

    select *
    from {{ source('analytics', 'mssql__core_dbo_clearing_files') }}

),

renamed as (

    select *

    from source

)

select * from renamed
