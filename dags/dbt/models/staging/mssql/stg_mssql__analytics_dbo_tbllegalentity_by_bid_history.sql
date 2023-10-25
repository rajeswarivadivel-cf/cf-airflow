with source as (

    select *
    from {{ source('analytics', 'mssql__analytics_dbo_tbllegalentity_by_bid_history') }}

),

renamed as (

    select *

    from source

)

select * from renamed
