with source as (

    select * from {{ source('analytics', 'mssql__core_dbo_saledisputes') }}

),

renamed as (

    select *

    from source

)

select * from renamed
