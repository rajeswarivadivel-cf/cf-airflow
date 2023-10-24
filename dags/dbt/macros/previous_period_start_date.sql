{% macro previous_period_start_date() %}
	(select cast(date_trunc('month', current_date) - interval '1 month' as date))
{% endmacro %}