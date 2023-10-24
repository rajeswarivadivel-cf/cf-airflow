with
merchants as (
    select distinct
        businessid as merchant_id,
        firsttransactiondate as merchant_created_on
    from
        {{ ref('stg_mssql__analytics_dbo_tbldatalake') }}
    where
        lower(relationship) = 'direct'
        and lower(merchantcountrycode) = 'gb'
        and businessid != 444595
        and lower(transactiontype) in (
            'sale', 'sale with cashback', 'refund', 'representment'
        )
        and lower(transactionstatus) in ('paid', 'reserved', 'commissioned')
),

companies as (
    select
        merchant_id,
        merchant_name as company_name,
        legal_entity_id as company_type,
        merchant_reg_number as company_registration_number,
        regexp_replace(
            trim(merchant_address_line1) || ','
            || trim(merchant_address_line2) || ','
            || trim(merchant_address_line3) || ','
            || trim(merchant_address_line4) || ','
            || trim(merchant_city) || ','
            || trim(merchant_country_2_char) || ','
            || trim(merchant_zip), ',+', ','
        ) as company_address,
        cast(null as float) as company_expected_monthly_payments_value,
        merchant_sig_first_name as director_first_name,
        merchant_sig_last_name as director_last_name,
        cast(
            replace(merchant_sig_dob, '', null) as date
        ) as director_date_of_birth
    from
        {{ ref('stg_mssql__core_dbo_merchants') }}
),

banks as (
    select
        payee_id,
        case
            when
                owners.external_ref similar to '[0-9]+'
                then cast(owners.external_ref as int)
        end as merchant_id,
        cast(
            regexp_replace(bank_sortcode, '[- ]', '') as varchar
        ) as bank_sort_code,
        bank_account_number,
        cast(null as date) as bank_details_last_modified_on,
        row_number()
            over (partition by merchant_id order by payee_id desc)
        as rank
    from
        {{ ref('stg_mssql__accounts_dbo_acc_bank_payees') }} as bank_payees
    left join
        {{ ref('stg_mssql__accounts_dbo_acc_account_groups') }} as acc_groups
        on bank_payees.account_group_id = acc_groups.account_group_id
    left join {{ ref('stg_mssql__accounts_dbo_acc_owners') }} as owners
        on acc_groups.owner_id = owners.owner_id
),

latest_banks as (
    select *
    from
        banks
    where
        rank = 1
),

address_formats as (
    select
        merchant_id,
        merchant_country_2_char as country,
        trim(merchant_address_line1) as address_line_1,
        regexp_replace(regexp_replace(
            trim(merchant_address_line2) || ','
            || trim(merchant_address_line3) || ','
            || trim(merchant_address_line4), ',+', ','
        ), ',$', '') as address_line_2,
        merchant_city as town_or_city,
        merchant_zip as postcode
    from
        {{ ref('stg_mssql__core_dbo_merchants') }}
),

joined as (
    select
        m.merchant_id,
        m.merchant_created_on,
        '' as applicant_first_name,
        '' as applicant_last_name,
        '' as applicant_date_of_birth,
        '' as applicant_home_address,
        '' as applicant_email_address,
        '' as applicant_phone,
        '' as applicant_ni_or_passport_or_driving_license_number,
        '' as applicant_vat_number,
        c.company_name,
        c.company_type,
        c.company_registration_number,
        c.company_address as company_registration_address,
        c.company_address as company_trading_address,
        '' as company_sic_code,
        c.company_expected_monthly_payments_value,
        '' as ubo_first_name,
        '' as ubo_middle_name,
        '' as ubo_last_name,
        '' as ubo_date_of_birth,
        '' as ubo_residential_address,
        '' as ubo_percentage_ownership,
        '' as director_first_name,
        '' as director_middle_name,
        '' as director_last_name,
        cast(null as date) as director_date_of_birth,
        '' as bank_name,
        '' as bank_account_type,
        b.bank_sort_code,
        b.bank_account_number,
        b.bank_details_last_modified_on,
        af.country,
        af.address_line_1,
        af.address_line_2,
        af.town_or_city,
        af.postcode
    from
        merchants as m
    left join companies as c
        on m.merchant_id = c.merchant_id
    left join latest_banks as b
        on m.merchant_id = b.merchant_id
    left join address_formats as af
        on m.merchant_id = af.merchant_id
)

select *
from
    joined
