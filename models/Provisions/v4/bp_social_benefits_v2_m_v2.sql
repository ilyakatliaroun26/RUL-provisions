
{{
  config(
    materialized = "incremental",
    dist='user_id',
    sort='etl_updated',
    unique_key="unique_key",
    tags=["retail_provisions"]
    )
}}

-- dbt run --full-refresh --select bp_social_benefits_v2_m_v2 (first time)
-- dbt run --select bp_social_benefits_v2_m_v2 (other runs)

with

social_benefits as (
SELECT  
ua.user_id
, max(t.created) as latest_sb_date
FROM etl_reporting.zr_transaction t 
LEFT JOIN cr_account a ON t.account_id = a.id 
INNER JOIN cr_user_account ua USING(account_id) 
WHERE bank_balance_impact > 0 
      AND t.partner_name LIKE '%Bundesagentur%Arbeit%' 
      AND t.partner_name NOT LIKE '%Familienkasse%' 
      AND ' / ' || SUBSTRING(reference_text, 1, 5) = SUBSTRING(reference_text, 15,8) /* ALG2 */ 
      AND a.account_role = 'PRIMARY' AND TRIM(a.status) <> 'CLOSED' 
      AND t.created::date between date_add('day', - 30, last_day(date_add('month'
                                                                        , -1
                                                                        , last_day(getdate())
                                                                    )
                                                                )
                                        )::date and last_day(
                                                            date_add('month'
                                                                    , -1
                                                                    , last_day(getdate())
                                                                    )
                                                            ) 
group by 1
)

select * 
, last_day(
        date_add('month'
                , -1
                , last_day(getdate())
                )
        ) as reporting_date
, getdate() as etl_updated
, coalesce(user_id, '') || coalesce(etl_updated::varchar, '') as unique_key
from social_benefits
{% if is_incremental() %}
where etl_updated > (select max(etl_updated) from {{this}})
{% endif %}
