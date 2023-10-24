with source as (

    select *
    from
        {{ source('analytics', 'mssql__analytics_dbo_vw_bidbysalesperson_secondary_15thcutoff') }}

),

renamed as (

    select *

    from source

)

select * from renamed
