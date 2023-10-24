with source as (

    select *
    from {{ source('analytics', 'mssql__analytics_dev_dbo_vw_bid_mcc') }}

),

renamed as (

    select *

    from source

)

select * from renamed
