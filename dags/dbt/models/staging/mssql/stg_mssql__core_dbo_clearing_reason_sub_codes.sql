with source as (

    select *
    from {{ source('analytics', 'mssql__core_dbo_clearing_reason_sub_codes') }}

),

renamed as (

    select *

    from source

)

select * from renamed
