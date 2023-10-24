{% macro prev_quarter_end_date() %}
    cast(date_trunc('quarter', current_date) - interval '1 day' as date)
{% endmacro %}