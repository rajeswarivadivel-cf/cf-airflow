with source as (

    select *
    from {{ source('analytics', 'mssql__accounts_dbo_tlkptransactiontype') }}

),

renamed as (

    select *

    from source

)

select * from renamed
