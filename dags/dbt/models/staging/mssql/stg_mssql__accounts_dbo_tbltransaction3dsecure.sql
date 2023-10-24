with source as (

    select *
    from {{ source('analytics', 'mssql__accounts_dbo_tbltransaction3dsecure') }}

),

renamed as (

    select *

    from source

)

select * from renamed
