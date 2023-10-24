select
    date_part(year, transactiondate) as year,
    date_part(month, transactiondate) as month,
    businessid as bid,
    sum(isnull(totalrevenuegbp, 0)) as acqtotalrevenuegbp,
    0 as gatewayrevenuegbp,
    0 as pcirevenuegbp,
    0 as accountupdatertotalrevenuegbp,
    0 as fxtotalrevenuegbp,
    0 as manualearlysettlementgbp,
    sum(isnull(esmarkupamountgbp, 0)) as autoearlysettlementrevenuegbp,
    0 as totalpartnercommissiongbp,
    sum(isnull(interchangegbp, 0)) as ic_costs,
    sum(isnull(schemefeessalegbp, 0))
    + sum(isnull(schemefeesnonsalegbp, 0)) as sf_costs,
    0 as revenuegbp,
    0 as thirdpartygatewaycosts,
    0 as cosgbp,
    0 as salescommissions,
    '' as comments,
    sum(isnull(totalturnovergbp, 0)) as totalturnovergbp,
    0 as totalcosgbp,
    0 as remittancefeerevenuegbp,
    0 as remittancefeecostgbp,
    sum(transactioncount) as transactioncount,
    0 as value_allocated_scheme_fees,
    0 as value_allocated_interchange,
    0 as "3ds cost gbp",
    0 as "3ds revenue gbp"
from
    {{ ref('stg_mssql__analytics_dbo_tblfinancialreport') }}
group by
    date_part(year, transactiondate),
    date_part(month, transactiondate),
    businessid
