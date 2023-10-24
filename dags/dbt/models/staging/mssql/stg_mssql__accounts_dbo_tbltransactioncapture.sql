with source as (

    select *
    from {{ source('analytics', 'mssql__accounts_dbo_tbltransactioncapture') }}

),

renamed as (

    select *

    from source

)

select * from renamed
