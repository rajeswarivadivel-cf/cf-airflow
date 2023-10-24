with source as (

    select * from {{ source('analytics', 'mssql__core_dbo_acquirer_bin') }}

),

renamed as (

    select *

    from source

)

select * from renamed
