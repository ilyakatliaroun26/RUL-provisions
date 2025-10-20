

-- dbt run --full-refresh --select bp_default_dunning_m_v2 (first time)
-- dbt run --select bp_default_dunning_m_v2 (other runs)

with

lanthanum_action_log as (
select 
l.user_id
, l.created_at as created
, l.dunning_process_id
, la.name as action
from lanthanum_action_logs l 
inner join lanthanum_actions la on l.actions_id = la.id

union all 

select distinct
c.user_id 
, dp.created::date as created
, dp.dunning_process_id
, replace(ast.name,' ', '_') as action
from aspirin_action_log as dp
inner join carbonium_user_account c on c.account_id = dp.account_id
inner join aspirin_dunning_process_task_definition t on t.id = dp.task_id
inner join aspirin_dunning_process_step_definition ast on t.dunning_process_step = ast.id
)



, dunning_cancellations as (
SELECT  user_id
       , MAX(CASE WHEN action in ('SECOND_OFFICIAL_NOTIFICATION', 'SECOND_OFFICIAL_REMINDER') THEN created 
                  ELSE NULL END) AS default_date 
FROM lanthanum_action_log  
WHERE dunning_process_id IN (SELECT dunning_process_id
                             FROM lanthanum_action_log
                             WHERE action in ('SECOND_OFFICIAL_NOTIFICATION', 'SECOND_OFFICIAL_REMINDER')
                                   AND created::date <= last_day(
                                                                date_add('month'
                                                                        , -1
                                                                        , last_day(getdate())
                                                                        )
                                                                )
                                                                        EXCEPT (SELECT dunning_process_id 
                                                                                FROM lanthanum_action_log
                                                                                WHERE action = 'USER_EXITS_FROM_ARREARS' 
                                                                                AND created::date <= last_day(
                                                                                                            date_add('month'
                                                                                                                    , -1
                                                                                                                    , last_day(getdate())
                                                                                                                    )
                                                                                                            ) 
                                                                                                            EXCEPT SELECT dunning_process_id 
                                                                                                                            FROM lanthanum_action_log  
                                                                                                                            WHERE action in ('SEND_CLOSURE_EMAIL','ACCOUNT_CLOSED_EMAIL') ) )
GROUP BY  user_id 
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
from dunning_cancellations

where etl_updated > (select max(etl_updated) from "n26"."credit_risk_playground"."bp_default_dunning_m_v2")
