select
    t.external_ref,
    bagag.currency_code_alpha3 as settlement_currency
from {{ ref('stg_mssql__accounts_dbo_acc_transactions') }} as t
inner join
    {{ ref('stg_mssql__accounts_dbo_acc_account_groups') }} as ag
    on
        t.account_group_id = ag.account_group_id
        and lower(ag.account_group_type) = 'a'
left join
    {{ ref('stg_mssql__accounts_dbo_acc_account_groups') }} as bagag
    on ag.payment_account_group_id = bagag.account_group_id