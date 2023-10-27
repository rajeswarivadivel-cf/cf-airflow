{# Rebuild this table when there are new mssql tables created in Redshift. #}
{% set query %}
select distinct replace(table_name, 'deprecated_', '') from {{ ref('stg_mssql_table_record_counts') }}
{% endset %}

{% set results = run_query(query) %}

{% if execute %}
{% set mssql_tables = results.columns[0].values() %}
{% else %}
{% set mssql_tables = [] %}
{% endif %}

with

mssql_record_counts as (
    select
        replace(table_name, 'deprecated_', '') as table_name,
        created_at,
        record_count
    from {{ ref('stg_mssql_table_record_counts') }}
),

latest_mssql_record_counts as (
    select * from (
        select
            *,
            row_number()
                over (partition by table_name order by created_at desc)
            as rank
        from mssql_record_counts
    ) where rank = 1
),

redshift_record_counts as (
{% if mssql_tables|length > 0 %}
{% for mssql_table in mssql_tables %}
{% set table_name %}mssql__{{ mssql_table|replace(".", "_") }}{% endset %}
{% set relation = source('analytics', table_name) %}
    select
        '{{ mssql_table }}'::varchar as mssql_table,
        '{{ relation.identifier }}'::varchar as redshift_table,
        count(*) as redshift_count
    from
        {{ relation }}
{% if not loop.last %}
    union distinct
{% endif %}
{% endfor %}
{% else %}
    select
        null::varchar as mssql_table,
        null::varchar as redshift_table,
        null::int as redshift_count
{% endif %}
),

latest_record_counts as (
    select
        replace(mssql_table, 'deprecated_', '') as mssql_table,
        mssql_count,
        redshift_count
    from {{ ref('stg_mssql__latest_record_counts') }}
),

joined as (
    select
        mssql.table_name as mssql_table,
        rs.redshift_table,
        mssql.created_at as last_run_at,
        mssql.record_count as mssql_count,
        rs.redshift_count as redshift_count,
        rs.redshift_count - mssql.record_count as variance,
        latest.mssql_count as latest_mssql_count,
        latest.redshift_count as latest_redshift_count,
        latest.redshift_count - latest.mssql_count as latest_variance
    from latest_mssql_record_counts as mssql
    left join redshift_record_counts as rs
        on mssql.table_name = rs.mssql_table
    left join latest_record_counts as latest
        on mssql.table_name = latest.mssql_table
    order by mssql_table
)

select * from joined
