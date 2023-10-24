with source as (

    select * from {{ source('analytics', 'mssql__core_dbo_trans_details') }}

),

renamed as (

    select *

    from source

)

select * from renamed
