{% macro prev_quarter_start_date() %}
    cast(date_trunc('quarter', current_date) - interval '3 months' as date)
{% endmacro %}