with

report_period_start_date as (
    select cast(date_trunc('quarter', current_date) - interval '3 months' as date)
),

report_period_end_date as (
    select cast(date_trunc('quarter', current_date) - interval '1 day' as date)
),

report_year as (
    select extract(year from (select * from report_period_start_date))
),

report_quarter as (
    select extract(quarter from (select * from report_period_start_date))
),

-- Merchants that have any transaction in last quarter.
active_merchant_ids as (
    select distinct mid
    from
        {{ ref('stg_mssql__analytics_dbo_tbldatalake') }}
    where
        cast(postingtime as date) between (select * from report_period_start_date) and (select * from report_period_end_date)
),

stores as (
    select
        m.legal_entity_id,
        m.merchant_id,
        m.merchant_type,
        s.store_id,
        s.status,
        cast(s.golive_date as date) as golive_date,
        cast(closed_suspended_dt as date) as closed_suspended_date
    from
        {{ ref('stg_mssql__core_dbo_stores') }} as s
    left join {{ ref('stg_mssql__core_dbo_merchants') }} as m
        on s.merchant_id = m.merchant_id
),

valid_stores as (
    select *
    from
        stores
    where
        store_id in (
            select store_id from {{ ref('stg_mssql__core_dbo_terminal_acquirers') }}
        )
        and merchant_id in (
            select merchant_id from {{ ref('stg_mssql__core_dbo_terminal_acquirers') }}
        )
),

operating_stores as (
    select *
    from
        stores
    where
        status = 2
        and store_id in (select mid from active_merchant_ids)
),

pos_terminals as (
    select
        s.store_id,
        ta.terminal_id,
        ta.acquirer_id
    from
        stores as s
    left join
        {{ ref('stg_mssql__core_dbo_terminal_acquirers') }} as ta
        on s.store_id = ta.store_id
    where
        s.status = 2
        and s.merchant_type = 6
        and ta.terminal_status = 1
        and ta.acquirer_id in (9, 10, 17)
        and ta.trans_class_id = -2
),

store_counts_by_merchant_id as (
    select
        t.*,
        m.legal_entity_id
    from
        (
            select
                s.merchant_id,
                count(distinct store_id) as total_count,
                count(distinct(
                    case
                        when
                            status = 2
                            and golive_date between (select * from report_period_start_date) and (select * from report_period_end_date)
                            then store_id
                    end
                )) as opened_store_count,
                count(distinct(
                    case
                        when
                            status = 0
                            and closed_suspended_date between (select * from report_period_start_date) and (select * from report_period_end_date)
                            then store_id
                    end
                )) as closed_store_count
            from
                stores as s
            group by
                s.merchant_id
        ) as t
    left join {{ ref('stg_mssql__core_dbo_merchants') }} as m on t.merchant_id = m.merchant_id
),

transactions as (
    select
        qmr_reporting_bucket,
        debit_credit,
        legal_entity,
        lower(transaction_type) = 'refund' as is_refund,
        sum(transaction_count) as transaction_count,
        sum(transaction_amount_gbp) as transaction_amount_gbp
    from
        {{ ref('rpt_qmr_mastercard_transaction_summary') }}
    where
        posting_year = (select * from report_year)
        and posting_quarter = (select * from report_quarter)
    {{ dbt_utils.group_by(n=4) }}
),

r10c4 as (
    select
        count(distinct merchant_id)
    from
        operating_stores
    where
        legal_entity_id in (0, 10)
),

r11c4 as (
    select
        count(distinct merchant_id)
    from
        store_counts_by_merchant_id
    where
        opened_store_count = total_count
        and legal_entity_id in (0, 10)
),

r12c4 as (
    select
        count(distinct merchant_id)
    from
        store_counts_by_merchant_id
    where
        closed_store_count = total_count
        and legal_entity_id in (0, 10)
),

r13c4 as (
    select
        count(distinct store_id)
    from
        operating_stores
    where
        legal_entity_id in (0, 10)
),

r14c4 as (
    select
        count(distinct store_id)
    from
        valid_stores
    where
        status = 2
        and golive_date between (select * from report_period_start_date) and (select * from report_period_end_date)
        and legal_entity_id in (0, 10)
),

r15c4 as (
    select
        count(distinct store_id)
    from
        valid_stores
    where
        status = 0
        and closed_suspended_date between (select * from report_period_start_date) and (select * from report_period_end_date)
        and legal_entity_id in (0, 10)
),

r16c4 as (
    select
        count(distinct terminal_id)
    from
        pos_terminals
    where
        acquirer_id in (0, 10)
),

r17c4 as (
    select * from r16c4
),

r18c4 as (
    select * from r16c4
),

r19c4 as (
    select
        count(distinct store_id)
    from
        pos_terminals
    where
        acquirer_id in (0, 10)
),

r20c4 as (
    select * from r16c4
),

r30c4 as (
    select sum(transaction_count)
    from transactions
    where
        lower(qmr_reporting_bucket) = 'domestic interchange acquiring'
        and lower(debit_credit) = 'credit'
        and not is_refund
),

r30c5 as (
    select sum(transaction_amount_gbp)
    from transactions
    where
        lower(qmr_reporting_bucket) = 'domestic interchange acquiring'
        and lower(debit_credit) = 'credit'
        and not is_refund
),

r31c4 as (
    select sum(transaction_count)
    from transactions
    where
        lower(qmr_reporting_bucket) = 'international acquiring within region'
        and lower(debit_credit) = 'credit'
        and not is_refund
),

r31c5 as (
    select sum(transaction_amount_gbp)
    from transactions
    where
        lower(qmr_reporting_bucket) = 'international acquiring within region'
        and lower(debit_credit) = 'credit'
        and not is_refund
),

r32c4 as (
    select sum(transaction_count)
    from transactions
    where
        lower(qmr_reporting_bucket) = 'international acquiring outside of region'
        and lower(debit_credit) = 'credit'
        and not is_refund
),

r32c5 as (
    select sum(transaction_amount_gbp)
    from transactions
    where
        lower(qmr_reporting_bucket) = 'international acquiring outside of region'
        and lower(debit_credit) = 'credit'
        and not is_refund
),

r44c4 as (
    select sum(transaction_count)
    from transactions
    where
        lower(qmr_reporting_bucket) = 'domestic interchange acquiring'
        and lower(debit_credit) = 'credit'
        and is_refund
),

r44c5 as (
    select sum(transaction_amount_gbp)
    from transactions
    where
        lower(qmr_reporting_bucket) = 'domestic interchange acquiring'
        and lower(debit_credit) = 'credit'
        and is_refund
),

r45c4 as (
    select sum(transaction_count)
    from transactions
    where
        lower(qmr_reporting_bucket) = 'international acquiring within region'
        and lower(debit_credit) = 'credit'
        and is_refund
),

r45c5 as (
    select sum(transaction_amount_gbp)
    from transactions
    where
        lower(qmr_reporting_bucket) = 'international acquiring within region'
        and lower(debit_credit) = 'credit'
        and is_refund
),

r46c4 as (
    select sum(transaction_count)
    from transactions
    where
        lower(qmr_reporting_bucket) = 'international acquiring outside of region'
        and lower(debit_credit) = 'credit'
        and is_refund
),

r46c5 as (
    select sum(transaction_amount_gbp)
    from transactions
    where
        lower(qmr_reporting_bucket) = 'international acquiring outside of region'
        and lower(debit_credit) = 'credit'
        and is_refund
),

r55c4 as (
    select sum(transaction_count)
    from transactions
    where
        lower(qmr_reporting_bucket) = 'domestic interchange acquiring'
        and lower(debit_credit) = 'debit'
        and not is_refund
),

r55c5 as (
    select sum(transaction_amount_gbp)
    from transactions
    where
        lower(qmr_reporting_bucket) = 'domestic interchange acquiring'
        and lower(debit_credit) = 'debit'
        and not is_refund
),

r56c4 as (
    select sum(transaction_count)
    from transactions
    where
        lower(qmr_reporting_bucket) = 'international acquiring within region'
        and lower(debit_credit) = 'debit'
        and not is_refund
),

r56c5 as (
    select sum(transaction_amount_gbp)
    from transactions
    where
        lower(qmr_reporting_bucket) = 'international acquiring within region'
        and lower(debit_credit) = 'debit'
        and not is_refund
),

r57c4 as (
    select sum(transaction_count)
    from transactions
    where
        lower(qmr_reporting_bucket) = 'international acquiring outside of region'
        and lower(debit_credit) = 'debit'
        and not is_refund
),

r57c5 as (
    select sum(transaction_amount_gbp)
    from transactions
    where
        lower(qmr_reporting_bucket) = 'international acquiring outside of region'
        and lower(debit_credit) = 'debit'
        and not is_refund
),

r69c4 as (
    select sum(transaction_count)
    from transactions
    where
        lower(qmr_reporting_bucket) = 'domestic interchange acquiring'
        and lower(debit_credit) = 'debit'
        and is_refund
),

r69c5 as (
    select sum(transaction_amount_gbp)
    from transactions
    where
        lower(qmr_reporting_bucket) = 'domestic interchange acquiring'
        and lower(debit_credit) = 'debit'
        and is_refund
),

r70c4 as (
    select sum(transaction_count)
    from transactions
    where
        lower(qmr_reporting_bucket) = 'international acquiring within region'
        and lower(debit_credit) = 'debit'
        and is_refund
),

r70c5 as (
    select sum(transaction_amount_gbp)
    from transactions
    where
        lower(qmr_reporting_bucket) = 'international acquiring within region'
        and lower(debit_credit) = 'debit'
        and is_refund
),

r71c4 as (
    select sum(transaction_count)
    from transactions
    where
        lower(qmr_reporting_bucket) = 'international acquiring outside of region'
        and lower(debit_credit) = 'debit'
        and is_refund
),

r71c5 as (
    select sum(transaction_amount_gbp)
    from transactions
    where
        lower(qmr_reporting_bucket) = 'international acquiring outside of region'
        and lower(debit_credit) = 'debit'
        and is_refund
),

lines as (
{% for n, c1, c2, c3, c4, c5, c6, c7 in [
    (1, 'Year', 'Quarter', 'Brand', 'ICA', 'Currency', 'Program', ''),
    (2, '2023', '2', 'Mastercard', '13908', 'GBP', 'Mastercard Acceptance', 'Cashflows'),
    (3, '', '', '', '', '', '', ''),
    (4, 'I. Acceptance', '', '', '', '', '', ''),
    (5, 'A. Cash Disbursement Locations', '', '', 'Total', '', '', ''),
    (6, '1. Number of branch locations where cash can be obtained', '', '', '', '', '', ''),
    (7, '2. Number of ATMs', '', '', '', '', '', ''),
    (8, '2a. Number of MasterCard-approved EMV chip compliant ATMs accepting MasterCard cards (Non MC-approved chip ATMs should NOT be included here)', '', '', '', '', '', ''),
    (9, 'B. Merchants', '', '', 'Total', '', '', ''),
    (10, '1. Number of MasterCard merchants', '', 'cte(r10c4)', '', '', '', ''),
    (11, '2. Number of new merchants added this quarter', '', 'cte(r11c4)', '', '', '', ''),
    (12, '3. Number of merchants lost this quarter', '', 'cte(r12c4)', '', '', '', ''),
    (13, '4. Total merchant locations', '', 'cte(r13c4)', '', '', '', ''),
    (14, '5. Number of new merchant locations added this quarter', '', 'cte(r14c4)', '', '', '', ''),
    (15, '6. Number of merchant locations lost this quarter', '', 'cte(r15c4)', '', '', '', ''),
    (16, '7. Number of POS (Point-Of-Sale) terminals at your merchant locations', '', 'cte(r16c4)', '', '', '', ''),
    (17, '7a. Number of MasterCard-approved EMV chip terminals (with or without PIN pad) at your merchant locations (Non MC-approved chip terminals should NOT be included here)', '', 'cte(r17c4)', '', '', '', ''),
    (18, '7b. Number of MasterCard-approved EMV chip terminals WITH A PIN PAD at your merchant locations (Non MC-approved chip terminals should NOT be included here)', '', 'cte(r18c4)', '', '', '', ''),
    (19, '8. Number of merchant locations accepting MasterCard contactless payment devices (i.e. cards, mobile devices, form factors)', '', 'cte(r19c4)', '', '', '', ''),
    (20, '9. Number of MasterCard-approved contactless terminals accepting MasterCard contactless payment devices (i.e. cards, mobile devices, form factors)', '', 'cte(r20c4)', '', '', '', ''),
    (21, 'C. Merchant Location Detail', '', '', 'Total', '', '', ''),
    (22, '1. Number of Mastercard QR Locations', 'NA', 'NA', '', '', '', ''),
    (23, '', '', '', '', '', '', ''),
    (24, 'Year', 'Quarter', 'Brand', 'ICA', 'Currency', 'Program', ''),
    (25, 'cte(report_year)', 'cte(report_quarter)', 'Mastercard', '13908', 'GBP', 'Mastercard Acquiring Credit', ''),
    (26, '', '', '', '', '', '', ''),
    (27, 'I. Retail Sales (Purchases)', '', 'Transactions', 'Volume', '', '', ''),
    (28, '1. Domestic On-us Acquiring', 'NA', '', '', '', '', ''),
    (29, '2. Domestic Other Brand / Non-MasterCard Processed Acquiring', 'NA', '', '', '', '', ''),
    (30, '3. Domestic Interchange Acquiring', 'NA', 'cte(r30c4)', 'cte(r30c5)', '', '', ''),
    (31, '4. International Acquiring Within Region', 'NA', 'cte(r31c4)', 'cte(r31c5)', '', '', ''),
    (32, '5. International Acquiring Outside of Region', 'NA', 'cte(r32c4)', 'cte(r32c5)', '', '', ''),
    (33, '6. Total (A1+A2+A3+A4+A5)', 'NA', '', '', '', '', ''),
    (34, 'II. Total Cash Advances', '', 'Transactions', 'Volume', '', '', ''),
    (35, '1. Domestic On-us Acquiring', 'NA', '', '', '', '', ''),
    (36, '2. Domestic Other Brand / Non-MasterCard Processed Acquiring', 'NA', '', '', '', '', ''),
    (37, '3. Domestic Interchange Acquiring', 'NA', '', '', '', '', ''),
    (38, '4. International Acquiring Within Region', 'NA', '', '', '', '', ''),
    (39, '5. International Acquiring Outside of Region', 'NA', '', '', '', '', ''),
    (40, '6. Total (A1+A2+A3+A4+A5)', 'NA', '', '', '', '', ''),
    (41, 'III. Refunds / Returns / Credits', '', 'Transactions', 'Volume', '', '', ''),
    (42, '1. Domestic On-us Acquiring', 'NA', '', '', '', '', ''),
    (43, '2. Domestic Other Brand / Non-MasterCard Processed Acquiring', 'NA', '', '', '', '', ''),
    (44, '3. Domestic Interchange Acquiring', 'NA', 'cte(r44c4)', 'cte(r44c5)', '', '', ''),
    (45, '4. International Acquiring Within Region', 'NA', 'cte(r45c4)', 'cte(r45c5)', '', '', ''),
    (46, '5. International Acquiring Outside of Region', 'NA', 'cte(r46c4)', 'cte(r46c5)', '', '', ''),
    (47, '6. Total (A1+A2+A3+A4+A5)', 'NA', '', '', '', '', ''),
    (48, '', '', '', '', '', '', ''),
    (49, 'Year', 'Quarter', 'Brand', 'ICA', 'Currency', 'Program', ''),
    (50, 'cte(report_year)', 'cte(report_quarter)', 'Mastercard', '13908', 'GBP', 'Mastercard Acquiring Debit', ''),
    (51, '', '', '', '', '', '', ''),
    (52, 'I. Retail Sales (Purchases)', '', 'Transactions', 'Volume', '', '', ''),
    (53, '1. Domestic On-us Acquiring', 'NA', '', '', '', '', ''),
    (54, '2. Domestic Other Brand / Non-MasterCard Processed Acquiring', 'NA', '', '', '', '', ''),
    (55, '3. Domestic Interchange Acquiring', 'NA', 'cte(r55c4)', 'cte(r55c5)', '', '', ''),
    (56, '4. International Acquiring Within Region', 'NA', 'cte(r56c4)', 'cte(r56c5)', '', '', ''),
    (57, '5. International Acquiring Outside of Region', 'NA', 'cte(r57c4)', 'cte(r57c5)', '', '', ''),
    (58, '6. Total (A1+A2+A3+A4+A5)', 'NA', '', '', '', '', ''),
    (59, 'II. Total Cash Advances', '', 'Transactions', 'Volume', '', '', ''),
    (60, '1. Domestic On-us Acquiring', 'NA', '', '', '', '', ''),
    (61, '2. Domestic Other Brand / Non-MasterCard Processed Acquiring', 'NA', '', '', '', '', ''),
    (62, '3. Domestic Interchange Acquiring', 'NA', '', '', '', '', ''),
    (63, '4. International Acquiring Within Region', 'NA', '', '', '', '', ''),
    (64, '5. International Acquiring Outside of Region', 'NA', '', '', '', '', ''),
    (65, '6. Total (A1+A2+A3+A4+A5)', 'NA', '', '', '', '', ''),
    (66, 'III. Refunds / Returns / Credits', '', 'Transactions', 'Volume', '', '', ''),
    (67, '1. Domestic On-us Acquiring', 'NA', '', '', '', '', ''),
    (68, '2. Domestic Other Brand / Non-MasterCard Processed Acquiring', 'NA', '', '', '', '', ''),
    (69, '3. Domestic Interchange Acquiring', 'NA', 'cte(r69c4)', 'cte(r69c5)', '', '', ''),
    (70, '4. International Acquiring Within Region', 'NA', 'cte(r70c4)', 'cte(r70c5)', '', '', ''),
    (71, '5. International Acquiring Outside of Region', 'NA', 'cte(r71c4)', 'cte(r71c5)', '', '', ''),
    (72, '6. Total (A1+A2+A3+A4+A5)', 'NA', '', '', '', '', '')
] %}
    select
    {% for i in [n, c1, c2, c3, c4, c5, c6, c7] %}
        {% set column_value %}
            {% if loop.index == 1 %}
                {{ i }}::int
            {% elif i[:3] == 'cte' %}
                {% if i[3] != '(' or i[-1] != ')' %}
                    {{ exceptions.raise_compiler_error('Invalid cte format. Got: ' ~ i) }}
                {% endif %}
                {% set cte_name = i[4:-1] %}
                (select * from {{ cte_name }})::varchar
            {% else %}
                '{{ i }}'::varchar
            {% endif %}
        {% endset %}
        {% set column_name %}
            {% if loop.index == 1 %}
                n
            {% else %}
                c{{ loop.index - 1 }}
            {% endif %}
        {% endset %}
        {{ column_value }} as {{ column_name }}{% if not loop.last %},{% endif %}
    {% endfor %}
    {% if not loop.last %}
        union all
    {% endif %}
{% endfor %}
),

ordered as (
    select c1, c2, c3, c4, c5, c6, c7
    from lines
    order by n
)

select * from ordered
