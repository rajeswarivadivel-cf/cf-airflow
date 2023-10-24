-- Since May-2023, data has been stopped inserting into Analytics_Dev.dbo.tblNonSaleSchemeFees.
-- Instead, data are inserted into Analytics.dbo.tblDataLake.
-- "The data that table holds is now directly pushed into our main MI table tbldatalake - but we haven't updated tbldatalake with the historic data it holds".
select
    year as year,
    month as month,
    merchant_id as bid,
    0 as acqtotalrevenuegbp,
    0 as gatewayrevenuegbp,
    0 as pcirevenuegbp,
    0 as accountupdatertotalrevenuegbp,
    0 as fxtotalrevenuegbp,
    0 as manualearlysettlementgbp,
    0 as autoearlysettlementrevenuegbp,
    0 as totalpartnercommissiongbp,
    0 as ic_costs,
    scheme_amount_fee_percent + fee_fixed as sf_costs,
    0 as revenuegbp,
    0 as thirdpartygatewaycosts,
    0 as cosgbp,
    0 as salescommissions,
    'Non Sale Scheme Fees' as comments,
    0 as totalturnovergbp,
    0 as totalcosgbp,
    0 as remittancefeerevenuegbp,
    0 as remittancefeecostgbp,
    0 as transactioncount,
    0 as value_allocated_scheme_fees,
    0 as value_allocated_interchange,
    0 as "3ds cost gbp",
    0 as "3ds revenue gbp"
from
    {{ ref('stg_crr_non_sales_scheme_fees_by_period_by_merchant') }}
