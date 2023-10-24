with source as (

    select *
    from {{ source('analytics', 'mssql__core_dbo_refundvoid_auth_responses') }}

),

renamed as (

    select *

    from source

)

select * from renamed
