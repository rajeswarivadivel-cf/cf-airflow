with
final as (
    select
        to_char(td.tran_time, 'YYYY') as year,
        to_char(td.tran_time, 'MM') as month,
        td.tran_ref,
        td.tran_cart_id as cart_id,
        td.store_id,
        td.tran_type,
        tt.transactiontype,
        td.auth_message,
        td.auth_code,
        td.auth_extra2 as schemeresponsecode,
        td.auth_acquirer_code as schemeresponsemsg,
        td.mpi_3dsversion,
        count(tran_ref) as auth_count
    from
        {{ ref('stg_mssql__core_dbo_trans_details') }} as td
    left join
        {{ ref('stg_mssql__accounts_dbo_tlkptransactiontype') }} as tt
        on td.tran_type = tt.transactiontypeid
    where
        td.tran_time
        >= cast(date_trunc('month', current_date) - interval '1 month' as date)
        and td.tran_time
        < cast(date_trunc('month', current_date) - interval '1 day' as date)
        and td.mpi_3dsversion is not null
        and td.merchant_id = 444595
    group by
        to_char(td.tran_time, 'YYYY'),
        to_char(td.tran_time, 'MM'),
        td.tran_ref,
        td.tran_cart_id,
        td.store_id,
        td.tran_type,
        tt.transactiontype,
        td.auth_message,
        td.auth_code,
        td.auth_extra2,
        td.auth_acquirer_code,
        td.mpi_3dsversion
)

select *
from
    final
