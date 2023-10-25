with source as (

    select *
    from {{ source('analytics', 'mssql__analytics_dbo_tblbidbysalesperson') }}

),

renamed as (

    select *

    from source

)

select * from renamed
