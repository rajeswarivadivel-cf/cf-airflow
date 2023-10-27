-- depends_on: {{ ref('int_salesperson_history') }}
/*This aims to replace analytics.core.tblbidbysalesperson_secondary_history.*/
{{ mart_partner_managers_by_bid_base(ref('stg_mssql__analytics_dbo_tblbidbysalesperson_secondary_history'), 'partner_manager_name_secondary') }}