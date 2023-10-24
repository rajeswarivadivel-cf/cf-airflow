with source as (

    select *
    from {{ source('analytics', 'mssql__accounts_dbo_acc_account_groups') }}

),

renamed as (

    select *

    from source

)

select * from renamed
