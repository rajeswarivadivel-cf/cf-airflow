with datalake_latest_by_transaction_id as (
    select * from (
        select
            *,
            row_number()
                over (partition by transactionid order by postingtime desc)
            as rank
        from {{ ref('stg_mssql__analytics_dbo_tbldatalake') }}
    )
    where rank = 1
),

final as (
    select
        d.transactionid,
        d.principalid,
        tch.interchangeratedescriptor as charge_interchangeratedescriptor,
        tch.interchangecurrency as charge_interchangecurrency,
        tch.interchangefxrate as charge_interchangefxrate,
        tch.interchangefixedfee as charge_interchangefixedfee,
        tch.interchangevariable as charge_interchangevariable,
        tch.interchangeamount as charge_interchangeamount,
        tch.interchangecharged as charge_interchangecharged,
        tch.interchangeamountgbp as charge_interchangeamountgbp,
        tch.schemefeescharged as charge_schemefeescharged,
        tch.schemeaamountgbp as charge_schemeaamountgbp,
        tch.schemebamountgbp as charge_schemebamountgbp,
        tch.schemecamountgbp as charge_schemecamountgbp,
        tec.scheme as editcriteria_scheme,
        tec.afs as editcriteria_afs,
        tec.productid as editcriteria_productid,
        tec.productsubtype as editcriteria_productsubtype,
        tec.mcc as editcriteria_mcc,
        tec.feeprogramindicator as editcriteria_feeprogramindicator,
        tec.reimburattribute as editcriteria_reimburattribute,
        tec.posterminalcapability as editcriteria_posterminalcapability,
        tec.cardholderauthcapability as editcriteria_cardholderauthcapability,
        tec.terminaloperatingenvironment as editcriteria_terminaloperatingenvironment,
        tec.cardholderpresentdata as editcriteria_cardholderpresentdata,
        tec.cardpresentdata as editcriteria_cardpresentdata,
        tec.posentrymode as editcriteria_posentrymode,
        tec.cardholderidmethod as editcriteria_cardholderidmethod,
        tec.motoeci as editcriteria_motoeci,
        tec.posenvironmentcode as editcriteria_posenvironmentcode,
        tec.authrespcode as editcriteria_authrespcode,
        tec.cvv2resultcode as editcriteria_cvv2resultcode,
        tec.servicecode as editcriteria_servicecode,
        tec.cabprogram as editcriteria_cabprogram,
        tec.ccflag as editcriteria_ccflag,
        tec.timeliness as editcriteria_timeliness
    from datalake_latest_by_transaction_id as d
    inner join {{ ref('stg_mssql__accounts_dbo_tbltransactioncapture') }} as tca
        on d.principalid = tca.principalid
    left join
        {{ ref('stg_mssql__accounts_dbo_tbltransactioneditcriteria') }} as tec
        on tca.principalid = tec.principalid
    left join {{ ref('stg_mssql__accounts_dbo_tbltransactioncharge') }} as tch
        on tca.transactioncaptureid = tch.transactioncaptureid
)

select *
from final
