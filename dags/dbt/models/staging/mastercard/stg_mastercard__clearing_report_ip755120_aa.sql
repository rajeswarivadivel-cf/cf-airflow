with source as (

    select * from {{ source('analytics', 'mc_clearing_report_ip755120_aa') }}

),

renamed as (

    select *

    from source

)

select * from renamed
