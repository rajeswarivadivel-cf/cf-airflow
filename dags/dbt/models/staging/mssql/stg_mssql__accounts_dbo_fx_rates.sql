with source as (

    select *
    from {{ source('analytics', 'mssql__accounts_dbo_fx_rates') }}

),

renamed as (

    select *

    from source

)

select * from renamed
