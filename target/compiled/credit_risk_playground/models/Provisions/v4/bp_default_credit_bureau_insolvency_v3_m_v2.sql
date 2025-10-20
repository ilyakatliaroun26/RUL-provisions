

 
with 
 
ca_merkmal AS (
SELECT merkmal_exploded.user_id,
       merkmal_exploded.provider,
       merkmal_exploded.rev_timestamp,
       merkmal_exploded.etl_updated,
       JSON_EXTRACT_PATH_TEXT(merkmal_exploded.merkmal, 'merkmalcode')  AS merkmalcode,
       JSON_EXTRACT_PATH_TEXT(merkmal_exploded.merkmal, 'beschreibung') AS merkmaldescription,
       merkmal_exploded.merkmal,
       merkmal_exploded.reaction_type
FROM credit_risk_playground.bp_default_credit_bureau_insolvency_me_v2 AS merkmal_exploded
)

, californium_credit_score_stg as (
select * 
, requested_on as rev_timestamp
, coalesce(lead(requested_on - interval '0.000001 second', 1) over (partition by user_id order by requested_on), '2100-01-01') as end_timestamp
from private.californium_credit_score_audit_log
order by user_id, requested_on
)

, californium_credit_score as (
select ccra.* 
from californium_credit_score_stg ccra
INNER JOIN "n26"."public"."dwh_cohort_dates" m ON m.end_time between ccra.rev_timestamp and  ccra.end_timestamp
where m.end_time::date = last_day(
                                date_add('month'
                                        , -1
                                        , last_day(getdate())
                                        )
                                        ) 
)

, schufa_curr as (
select distinct
              ccra.user_id 
              , last_day(
                        date_add('month'
                                , -1
                                , last_day(getdate())
                                )
                                )  as rev_timestamp
       from californium_credit_score ccra
       inner join dbt.zrh_users z using(user_id)
       inner join ca_merkmal csr
                       on ccra.user_id = csr.user_id
                       and csr.rev_timestamp::date = ccra.rev_timestamp::date
       where (
        (ccra.rating in ( 'N', 'O', 'P' )
             and (z.country_tnc = 'DEU' or z.country_tnc is null) 
             and ccra.provider like 'SCHUFA%' 
             and csr.merkmalcode in ( 'EV', 'HB', 'IA', 'IE', 'IS', 'RA', 'RV', 'S1', 'S2', 'S3' )
             and (csr.merkmal not like '%nachmeldegrund%' or
            (csr.merkmal not like '%lösch%' and csr.merkmal not like '%erledigungsvermerk%')))
       or 
        (z.country_tnc = 'AUT' 
            and ccra.provider like 'CRIF%'
            and ccra.rating in ('P'))
       ) 
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
from schufa_curr

where etl_updated > (select max(etl_updated) from "n26"."credit_risk_playground"."bp_default_credit_bureau_insolvency_v3_m_v2")
