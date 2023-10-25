with source as (

    select *
    from {{ source('analytics', 'mssql__analytics_dev_dbo_vw_mastermidlist') }}

),

renamed as (

    select *

    from source

)

select * from renamed
