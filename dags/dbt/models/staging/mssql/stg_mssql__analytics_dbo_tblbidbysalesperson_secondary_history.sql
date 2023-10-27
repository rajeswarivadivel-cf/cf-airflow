with source as (

    select *
    from {{ source('analytics', 'mssql__analytics_dbo_tblbidbysalesperson_secondary_history') }}

),

renamed as (

    select *

    from source

)

select * from renamed
