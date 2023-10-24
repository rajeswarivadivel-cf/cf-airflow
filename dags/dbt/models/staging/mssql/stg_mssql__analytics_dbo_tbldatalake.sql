with source as (

    select * from {{ source('analytics', 'mssql__analytics_dbo_tbldatalake') }}

),

renamed as (

    select *

    from source

)

select * from renamed
