

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
        , ps.created as calculated_at
        , row_number() over (
            partition by ps.user_id, pm.name || '-' || pm.version
            order by 
            ps.created,
                case 
                    when ps.score_type = 'behavioral' then 1
                    when ps.score_type = 'behavioral_unarranged' then 2
                    when ps.score_type = 'application_TBIL' then 3
                    when ps.score_type = 'application_CC' then 4
                    when ps.score_type = 'application_OD' then 5
                    else 6
                end
        ) as row_num
        , pd::float as pd 
        , rating_class
        , ps.meta.pit_calibration_beta as beta
        , ps.meta.insample_calibrated_pd::float as internal_pd
    from etl_reporting.porto_score ps
    inner join etl_reporting.porto_model pm on pm.id = ps.model_id
    inner join portfolio p on p.user_id = ps.user_id
    inner join dbt.zrh_users z on z.user_id = ps.user_id
    where pm.name || '-' || pm.version = 'unified-1.0'
      --    and ps.score_type in ('behavioral_unarranged', 'behavioral')
    and ps.created::date = last_day(date_add('month', -1 , last_day(getdate()))) 
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
    where row_num = 1
)

, porto_fixed as (
select 
p.user_id 
, p.calculated_at
, p.rating_class
, p.pd_2
, p.internal_pd
, case when beta = 0.279614 then 'DEU_MIX_WITH_UTILIZATION'
       when beta = -0.457881 then 'DEU_OD_WITH_UTILIZATION'
       when beta = -0.227762 then 'AUT_OD_WITH_UTILIZATION'
       when beta = -1.272583 then 'DEU_TBIL'
       when beta = -0.465253 then 'ESP_TBIL'
       when beta = -0.530291 then 'ITA_TBIL'
       when beta = 0.402868  then 'FRA_TBIL'
       when beta = -0.286839 then 'DEU_CC'
       when beta = -1.713889 then 'DEU_OD_WITHOUT_UTILIZATION'
       when beta = -1.218263 then 'AUT_OD_WITHOUT_UTILIZATION'
       when beta = -1.481441 then 'DEU_MIX_WITHOUT_UTILIZATION'
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
    
    where reporting_date > (select max(reporting_date) from "n26"."credit_risk_playground"."bp_monitoring_pd_supermodel_v2_m_v2")
    