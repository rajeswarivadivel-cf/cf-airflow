with source as (

    select *
    from
        {{ source('analytics', 'mssql__analytics_dbo_vw_partner_channel_15thcutoff') }}

),

renamed as (

    select *

    from source

)

select * from renamed
