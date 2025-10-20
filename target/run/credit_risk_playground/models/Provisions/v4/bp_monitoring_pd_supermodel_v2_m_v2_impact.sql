
      
  
    

  create  table
    "n26"."credit_risk_playground"."bp_monitoring_pd_supermodel_v2_m_v2_impact"
    
    diststyle key distkey (user_id)
    
      compound sortkey(reporting_date)
  as (
    

-- dbt run --full-refresh --select bp_monitoring_pd_supermodel_v2_m_v2 (first time)
-- dbt run --select bp_monitoring_pd_supermodel_v2_m_v2 (other runs)

with 

portfolio as (
select distinct 
user_id 
from credit_risk_playground.bp_portfolio_customers_aud_m_v2
where reporting_date::date = last_day(date_add('month', -1 , last_day(getdate()))) 

)

, pd as (
        select
        ps.user_id
        , z.country_tnc as tnc_country
        , '2025-08-31' as calculated_at
        , ps.pd::float as pd 
        , ps.rating_class
        , ps.beta as beta
        , ps.insample_pd::float as internal_pd
    from credit_risk_playground.bp_pd_monitoring_provisions_aug_31 ps
    inner join portfolio p on p.user_id = ps.user_id
    inner join dbt.zrh_users z on z.user_id = ps.user_id
    where ps.score_type = 'behavioral'
)

, porto as (
    select
        user_id,
        pd as pd_2,
        calculated_at,
        rating_class,
        beta, 
        internal_pd
    from pd
)

, porto_fixed as (
select 
p.user_id 
, p.calculated_at
, p.rating_class
, p.pd_2
, p.internal_pd
, case when beta = 1.151851 then 'DEU_MIX_WITH_UTILIZATION'
       when beta = -0.197961 then 'DEU_OD_WITH_UTILIZATION'
       when beta = -0.227762 then 'AUT_OD_WITH_UTILIZATION'
       when beta = 0.144261 then 'DEU_TBIL'
       when beta = -0.179664 then 'ESP_TBIL'
       when beta = -0.174885 then 'ITA_TBIL'
       when beta = 0.560843  then 'FRA_TBIL'
       when beta = -0.639250 then 'DEU_CC'
       when beta = -1.042866 then 'DEU_OD_WITHOUT_UTILIZATION'
       when beta = -1.218263 then 'AUT_OD_WITHOUT_UTILIZATION'
       when beta = -0.151521 then 'DEU_MIX_WITHOUT_UTILIZATION'
       when beta is null then 'UNARRANGED_OD'
       end as calibration_sgm

from porto p 

)


    select
    user_id
    , calculated_at
    , rating_class
    , pd_2
    , calibration_sgm
    , internal_pd
    , last_day(
                    date_add('month'
                            , -1
                            , last_day(getdate())
                            )
                            ) as reporting_date
    , getdate() as etl_updated
    , coalesce(user_id, '') || coalesce(getdate()::varchar, '') as unique_key
    from porto_fixed l
    
  );
  
  