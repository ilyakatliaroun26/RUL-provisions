

with 
californium_credit_score as (
select 
user_id 
, rating 
, provider
, requested_on as rev_timestamp
, coalesce(lead(requested_on - interval '0.000001 second', 1) over (partition by user_id order by requested_on), '2100-01-01') as end_timestamp
from private.californium_credit_score_audit_log
order by user_id, requested_on
)

, total as (
select distinct  a.user_id
                    , a.rating
                    , a.provider
                    , a.rev_timestamp
                    , last_day(
                            date_add('month'
                                    , -1
                                    , last_day(getdate())
                                    )
                                    ) as reporting_date
                    , getdate() as etl_updated
                    , coalesce(a.user_id, '') || coalesce(etl_updated::varchar, '') as unique_key 
           from californium_credit_score a 
           inner join "n26"."dbt"."zrh_users" z using(user_id)
           inner join ( select 
                       a.user_id
                        , b.rev_timestamp
                        , max(a.end_timestamp) as end_timestamp
               from californium_credit_score a
               inner join "n26"."dbt"."zrh_users" z using(user_id)
               inner join (select z.user_id
                                  , max(rev_timestamp::date) as rev_timestamp
                           from californium_credit_score ccra
                           inner join "n26"."dbt"."zrh_users" z using(user_id)
                           where (
                                  (
                                   (z.country_tnc = 'DEU' or z.country_tnc is null) 
                                   and ccra.provider like 'SCHUFA%' 
                                  ) 
                                  or 

                                  (
                                    z.country_tnc = 'AUT' 
                                    and ccra.provider like 'CRIF%'
                                    )
                                  )
                                 and rev_timestamp::date <= '2025-06-11'::date
                          group by z.user_id) b on a.user_id = b.user_id 
                                                 and a.rev_timestamp::date  = b.rev_timestamp::date 
                where  (
                                  (
                                   (z.country_tnc = 'DEU' or z.country_tnc is null) 
                                   and a.provider like 'SCHUFA%' 
                                  ) 
                                  or 

                                  (
                                    z.country_tnc = 'AUT' 
                                    and a.provider like 'CRIF%'
                                    )
                                  )
                group by 
                a.user_id
                , b.rev_timestamp) c on c.user_id = a.user_id 
                                         and c.rev_timestamp::date = a.rev_timestamp::date
                                         and c.end_timestamp::date = a.end_timestamp::date
            where (
                    (
                      (z.country_tnc = 'DEU' or z.country_tnc is null) 
                      and a.provider like 'SCHUFA%' 
                     ) 
                     or 

                    (
                      z.country_tnc = 'AUT' 
                      and a.provider like 'CRIF%'
                     )
                    )
    )
                    

  select * 
  from total 
    
  where  etl_updated > (select max(etl_updated) from "n26"."credit_risk_playground"."bp_credit_bureau_rating_m_ik")
    