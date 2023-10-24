with source as (

    select *
    from {{ source('analytics', 'mssql__accounts_dbo_tbltransactioncharge') }}

),

renamed as (

    select *

    from source

)

select * from renamed
