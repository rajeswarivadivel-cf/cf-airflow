select
	filename,
	min(settlementdate) as min_date,
	max(settlementdate) as max_date,
	count(*) as record_count
from
	analytics.mc_clearing_report_ip755120_aa
group by
	filename
order by
	min_date desc,
	max_date desc,
	filename desc