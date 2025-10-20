{{
  config(
    materialized = "incremental",
    dist='instrument_id',
    sort='etl_updated',
    unique_key="unique_key",
    tags=["retail_provisions"]
    )
}}

-- dbt run --full-refresh --select bp_forbearance_m_v2_v2 (first time)
-- dbt run --select bp_forbearance_m_v2_v2 (other runs)

with 


portfolio as (
select 
c.user_id 
, c.instrument_id 
, c.reportdate
, c.credit_creation_date
, c.product_flag
, c.TBIL_exposures + c.CC_exposures + c.RP_2_exposures + c.OD_exposures as bal
, coalesce(ps.pd
           , coalesce(ps_old.pd
                     , ps_lis.pd)) as InitialPD
from {{ ref('bp_retail_provisions_stg_v5_pre_updated_v2') }} c
left join dbt_pii.credit_score_audit_log ps on c.user_id = ps.user_id 
                                               and c.credit_creation_date::date = ps.calculated_at::date 
                                               and ps.model_name = 'PORTO'
                                               and ps.model_version = 'unified-1.0'
                                               and ps.score_type in ('behavioral')
left join dbt_pii.credit_score_audit_log ps_old on c.user_id = ps_old.user_id 
                                               and c.credit_creation_date::date = ps_old.calculated_at::date 
                                               and ps_old.model_name = 'PORTO'
                                               and ps_old.model_version = 'overdraft-1.1'
                                               and ps_old.score_type in ('behavioral_arranged')
left join dbt_pii.credit_score_audit_log ps_lis on c.user_id = ps_lis.user_id 
                                               and c.credit_creation_date::date = ps_lis.calculated_at::date 
                                               and ps_lis.model_name = 'LISBON'
                                               and ps_lis.score_type in ('behavioral_arranged', 'lisbon')
                                              
where reportdate::date = last_day(date_add('month', -1, last_day(getdate()))) 
      and c.product_flag in ('RP_2') -- rp ph.2
)


, californium_credit_score as (
select 
user_id 
, rating 
, provider
, requested_on as rev_timestamp
, coalesce(lead(requested_on - interval '0.000001 second', 1) over (partition by user_id order by requested_on), '2100-01-01') as end_timestamp
from private.californium_credit_score_audit_log
order by user_id, requested_on
)

, paccount as (
 select 
 user_id 
, created
, active 
, coalesce(allowance_cents,0) as allowance_cents
, lag(case when active = 1 then allowance_cents
           else 0 end) OVER (PARTITION by z.user_id order by created) as previous_allowance_cents
from etl_reporting.pa_paccount p
inner join dbt.zrh_users z on p.account_id = z.account_id
)
, paccount_ts as ( 
select 
user_id 
, active
, allowance_cents
, created as rev_timestamp 
, coalesce( lead(created) OVER (PARTITION BY user_id ORDER BY created) + interval'-0.000001 seconds' 
                  , '2100-01-01'::timestamp) as end_timestamp 
from paccount
where (previous_allowance_cents IS NULL OR previous_allowance_cents <> allowance_cents)
)

, garnishment as (
  select
  customer_id
  , ng.status
  , ng.created 
  , ng.updated
  from nd_garnishment ng
  left join  nd_participant np  on ng.id = np.garnishment_id
  where np.type = 'RESPONDENT' --and ng.status = 'OPEN'
)

, total as (
select distinct
p.*
, coalesce(date_diff('day',d.start_in_arrears::date, p.credit_creation_date::date),0) as dpd_at_credit_decision
, c.rev_timestamp as cb_report_date
, c.provider as cb_provider 
, c.rating as cb_rating
-- hrc is not used, since such customers are not eligible for repayment plan
, case when coalesce(date_diff('day',d.start_in_arrears::date, p.credit_creation_date::date),0) > 30 then 'dpd_30+'
       when initialpd >= 0.2790 then 'RC_15+'
       -- includes financial difficulties (+ insolvency)
       when (c.provider like 'SCHUFA%' and c.rating in ('N', 'O', 'P')) then 'cb_financial_difficulty'
       when (c.provider like 'CRIF%' and c.rating in ('P')) then 'cb_financial_difficulty'
       when (pa.user_id is not null) then 'p_account'
       when m.initiator_user_id is not null then 'unemployment_benefits'
       when g.customer_id is not null then 'garnishment'
        else 'other' end as forbearance_reason
from portfolio p 
left join dbt.bp_arrears_reg_aud d on p.user_id = d.user_id 
                                      and p.credit_creation_date::timestamp between d.start_in_arrears::timestamp and d.end_in_arrears::timestamp
left join californium_credit_score  c on c.user_id = p.user_id 
                                      and p.credit_creation_date::timestamp between c.rev_timestamp::timestamp and c.end_timestamp::timestamp
left join paccount_ts pa on pa.user_id = p.user_id 
                            and p.credit_creation_date::date between pa.rev_timestamp::timestamp and pa.end_timestamp::timestamp
left join garnishment g on g.customer_id = p.user_id 
                           and (
                                  (
                                    g.status = 'CLOSE'
                                      and p.credit_creation_date::timestamp between g.created::timestamp and g.updated::timestamp
                                  ) -- garnishment was active at the application, but now closed
                                  or 
                                  (
                                    g.status = 'OPEN' and g.created::timestamp <= p.credit_creation_date::timestamp
                                  )
                                ) 
left join madrid_categorization m on m.initiator_user_id = p.user_id 
                                      and m.created::date between date_add('day', -91, p.credit_creation_date)::date and p.credit_creation_date::date
                                      and (m.detailed_category like '%PublicAssistance%') 
                                      and (m.detailed_category not like '%FamilySupportAllowance%')
                                      and (m.detailed_category not like '%PublicAssistanceWelfare%')
where (dpd_at_credit_decision > 30) 
       or (initialpd >= 0.2790) 
       or (c.provider like 'SCHUFA%' and c.rating in ('N', 'O', 'P')) 
       or (c.provider like 'CRIF%' and c.rating in ('P')) 
       or (pa.user_id is not null)
       or (g.customer_id is not null)
       or (m.initiator_user_id is not null)
order by user_id, instrument_id
)

  select * 
  , getdate() as etl_updated
  , coalesce(instrument_id, '') || coalesce(etl_updated::varchar, '') as unique_key 
  from total 
    {% if is_incremental() %}
  where  etl_updated > (select max(etl_updated) from {{this}})
    {% endif %}