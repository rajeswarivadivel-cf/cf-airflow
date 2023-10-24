{% macro previous_year_start_date() %}
	(select cast(date_trunc('year', current_date) - interval '1 year' as date))
{% endmacro %}