
{{
  config(
    materialized = "incremental",
    dist='instrument_id',
    sort='etl_updated',
    unique_key="unique_key",
    tags=["retail_provisions"]
    )
}}

-- dbt run --full-refresh --select bp_retail_provisions_stg_v5_pre_updated_v2 (first time)
-- dbt run --select bp_retail_provisions_stg_v5_pre_updated_v2 (other runs)

with 

first_enabled_date as (
select user_id 
, encoded_key
, min(rev_timestamp) as first_creation_date
, max(rev_timestamp) as last_creation_date
from {{ ref('bp_overdraft_limit_periods_v2') }}
where rev_timestamp <= date_add('minute'
                                                        , -1
                                                        , date_add('day'
                                                            , 1
                                                            , last_day(
                                                                    date_add('month'
                                                                        , -1
                                                                        , last_day(getdate())
                                                                        )
                                                                    )
                                                            
                                                                ) 
                                                           ) and amount_cents > 0 
group by 
user_id 
, encoded_key
)

, repayment as (
select parent_account_key
, min(due_date) as first_repayment
, max(due_date) as last_repayment
from  {{ source('public', 'mmbr_repayment') }}  
group by 1
)


, total as (
select distinct

-- 1. Customers base

c.user_id 
, c.encoded_key as Instrument_ID
, z.country_tnc as Country
, z.legal_entity as LegalEntity
, 'YES' as IS_IFRS -- "No" for TSY bonds and "Yes" for other instruments
,  last_day(date_add('month', -1, last_day(getdate()))) as ReportDate
,  last_day(date_add('month', -1, last_day(getdate()))) as CalcDate -- for retail same as eom, for mortgage 1-month lag
, current_date::date as Script_calculation_date
-- 2. Product flag
-- product

, case when coalesce(p.amount_cents,0) > 0  and rp.user_id is not null then 'RP_1'
       when mll.OD_type = 'Arranged Overdraft' and rp.user_id is not null then 'RP_1'
       when c.user_id  = 'b8d7ad25-1ca4-49c0-a188-0cfd280816a7' then 'Unarranged Overdraft'
       when coalesce(p.amount_cents,0) > 0 then 'Arranged Overdraft'
       when mll.user_id is not null then mll.OD_type
       when o.user_id is not null and o.product = 'Installment Loans'then 'TBIL'
       when o.user_id is not null and o.product = 'Consumer Credit'then 'CC'
       when o.user_id is not null and o.product = 'Repayment Plane (Phase 2)' then 'RP_2' 
       end product_flag

, case when coalesce(p.amount_cents,0) > 0  and rp.user_id is not null then 'Arranged Overdraft'
       when mll.OD_type = 'Arranged Overdraft' and rp.user_id is not null then 'Arranged Overdraft'
       when c.user_id  = 'b8d7ad25-1ca4-49c0-a188-0cfd280816a7' then 'Unarranged Overdraft'
       when coalesce(p.amount_cents,0) > 0 then 'Arranged Overdraft'
       when mll.user_id is not null then mll.OD_type
       when o.user_id is not null and o.product = 'Installment Loans'then 'TBIL'
       when o.user_id is not null and o.product = 'Consumer Credit'then 'CC'
       when o.user_id is not null and o.product = 'Repayment Plane (Phase 2)' then 'RP_2'
       end Instrument_Type

, 'Kredit' as Instrument 

-- 3. Creation date 

, case when product_flag in ('Arranged Overdraft', 'RP_1') then coalesce(f.first_creation_date::date,last_day(date_add('month', -1, last_day(getdate()))))
       when product_flag <> 'Unarranged Overdraft' then coalesce(li.creation_date::date,last_day(date_add('month', -1, last_day(getdate()))))
       else null end credit_creation_date

-- 4. DPD

, start_in_arrears::date as start_in_arrears
, case when start_in_arrears is not null then datediff('day', start_in_arrears::date, last_day(date_add('month', -1, last_day(getdate())))) + 1
       else 0 end dpd

-- 5. Default

, case when ins.user_id is not null then true else false end as insolvency 
, case when d.user_id is not null then true else false end as dunning 
, case when sch.user_id is not null then true else false end as schufa_insolvency
, case when rp.user_id is not null then true else false end as rep_plan_user

, case when d.user_id is not null or ins.user_id is not null or sch.user_id is not null then 1
       when dpd > 90 then 1
       else 0 end is_default


-- 6. Credit Bureau flags

, schr.rating as schufa_rating 
, schr.provider
, case when schr.rev_timestamp::date is not null then last_day(date_add('month', -1, last_day(getdate())))
       else null end as schufa_rating_date

-- 7. Limit

, case when c.user_id = 'b8d7ad25-1ca4-49c0-a188-0cfd280816a7' then 0 -- error in Plutonium
       else coalesce(p.amount_cents/100::numeric,0) end as "Limit"

, coalesce(mll.overdraft_lim,0) as last_limit_eur

, mll.max_date_neg_balance::date as last_limit_date

-- 8. Exposures

, case when product_flag = 'TBIL' then coalesce(o.balance,0)
       else 0 end as TBIL_exposures

, case when product_flag = 'CC' then coalesce(o.balance,0)
       else 0 end as CC_exposures

, case when product_flag = 'RP_2' then coalesce(o.balance,0)
       else 0 end as RP_2_exposures

, case when product_flag in ('Arranged Overdraft', 'Unarranged Overdraft', 'RP_1') then coalesce(o.balance,0)
       else 0 end as OD_exposures
-- 9. PD

-- Calibration segment

-- ! Important ! In Porto if customer eligible for different calibration segment, we choose the one with the max PD

, case when max(case when product_flag in ('RP_1') then 1 else 0 end) over(partition by c.user_id) = 1 then 'RP_phase_1'
       when max(case when product_flag in ('RP_2') then 1 else 0 end) over(partition by c.user_id) = 1 then 'RP_phase_2'
       when m.calibration_sgm is not null then calibration_sgm
       when product_flag = 'Unarranged Overdraft' then 'UNARRANGED_OD'
       end porto_calibration_segment

, case when max(case when product_flag in ('RP_1') then 1 else 0 end) over(partition by c.user_id) = 1 then 'RP_phase_1'
       when max(case when product_flag in ('RP_2') then 1 else 0 end) over(partition by c.user_id) = 1 then 'RP_phase_2'
       when m.calibration_sgm is not null then calibration_sgm
       when product_flag = 'Unarranged Overdraft' then 'UNARRANGED_OD'
       end pit_calibration_segment  

-- Available Porto PD

, case when pit_calibration_segment in ('RP_phase_2') then 0.10979381443298969
       when credit_creation_date = last_day(date_add('month', -1, last_day(getdate()))) 
            and product_flag not in ('Unarranged Overdraft', 'RP_2')
            and m.pd_2 is null then pd_app.pd_2
       else m.pd_2 end as internal_pd

,  case when pit_calibration_segment in ('RP_phase_2') then ln(0.10979381443298969/(1 - 0.10979381443298969))
        when credit_creation_date = last_day(date_add('month', -1, last_day(getdate()))) 
             and product_flag not in ('Unarranged Overdraft', 'RP_2')
             and m.pd_2 is null then ln(pd_app.pd_2::float/(1-pd_app.pd_2::float)) 
        else ln(m.pd_2::float/(1-m.pd_2::float)) end as logit

, case when pit_calibration_segment = 'RP_phase_2' then 0.10979381443298969
       when credit_creation_date = last_day(date_add('month', -1, last_day(getdate()))) 
            and product_flag not in ('Unarranged Overdraft', 'RP_2')
            and m.pd_2 is null then pd_app.pd_2
       else m.pd_2 end as pd_pit_calibrated

-- MEV scenarios

-- down 

, case when pit_calibration_segment = 'RP_phase_2' then 0.10979381443298969
       when product_flag = 'Arranged Overdraft' then 1 / (1 + exp(-0.993527844*logit))
       when product_flag = 'CC' then 1 / (1 + exp(-0.9886120848*logit))
       when product_flag = 'RP_1' then 1 / (1 + exp(-0.9871234659*logit))
       when product_flag = 'TBIL' then 1 / (1 + exp(-0.8635598925*logit))
       when product_flag = 'Unarranged Overdraft' then 1 / (1 + exp(-0.942206167*logit)) end as pd_pit_calibrated_down

-- neu

, case when pit_calibration_segment = 'RP_phase_2' then 0.10979381443298969
       when product_flag = 'Arranged Overdraft' then 1 / (1 + exp(-1.002717242*logit))
       when product_flag = 'CC' then 1 / (1 + exp(-1.003832904*logit))
       when product_flag = 'RP_1' then 1 / (1 + exp(-1.0042727*logit))
       when product_flag = 'TBIL' then 1 / (1 + exp(-1.036338268*logit))
       when product_flag = 'Unarranged Overdraft' then 1 / (1 + exp(-1.02174169*logit)) end as pd_pit_calibrated_neu

-- up

, case when pit_calibration_segment = 'RP_phase_2' then 0.10979381443298969
       when product_flag = 'Arranged Overdraft' then 1 / (1 + exp(-1.011675526*logit))
       when product_flag = 'CC' then 1 / (1 + exp(-1.01799547*logit))
       when product_flag = 'RP_1' then 1 / (1 + exp(-1.021116909*logit))
       when product_flag = 'TBIL' then 1 / (1 + exp(-1.104161529*logit))
       when product_flag = 'Unarranged Overdraft' then 1 / (1 + exp(-1.094211134*logit)) end as pd_pit_calibrated_up

, 2.5*pd_pit_calibrated_down/100 + 95*pd_pit_calibrated_neu/100 + 2.5*pd_pit_calibrated_up/100 as pd_pit_calibrated_scen_weighted

-- Calibration PD

,  case  when pd_pit_calibrated::float >= 0 and pd_pit_calibrated::float < 0.0001 then 1
         when pd_pit_calibrated::float >= 0.0001 and pd_pit_calibrated::float < 0.0005 then 2
         when pd_pit_calibrated::float >= 0.0005 and pd_pit_calibrated::float < 0.0010 then 3
         when pd_pit_calibrated::float >= 0.0010 and pd_pit_calibrated::float < 0.0016 then 4
         when pd_pit_calibrated::float >= 0.0016 and pd_pit_calibrated::float <  0.0024 then 5
         when pd_pit_calibrated::float >= 0.0024 and pd_pit_calibrated::float < 0.0039 then 6
         when pd_pit_calibrated::float >= 0.0039 and pd_pit_calibrated::float < 0.0074 then 7
         when pd_pit_calibrated::float >= 0.0074 and pd_pit_calibrated::float < 0.0143 then 8
         when pd_pit_calibrated::float >= 0.0143 and pd_pit_calibrated::float < 0.0226 then 9
         when pd_pit_calibrated::float >= 0.0226 and pd_pit_calibrated::float < 0.0322 then 10
         when pd_pit_calibrated::float >= 0.0322 and pd_pit_calibrated::float < 0.0430 then 11
         when pd_pit_calibrated::float >= 0.0430 and pd_pit_calibrated::float < 0.0562 then 12
         when pd_pit_calibrated::float >= 0.0562 and pd_pit_calibrated::float < 0.1119 then 13
         when pd_pit_calibrated::float >= 0.1119 and pd_pit_calibrated::float < 0.2790 then 14
         when pd_pit_calibrated::float >= 0.2790 and pd_pit_calibrated::float < 0.4601 then 15
         when pd_pit_calibrated::float >= 0.4601 and pd_pit_calibrated::float < 0.6087 then 16
         when pd_pit_calibrated::float >= 0.6087 and pd_pit_calibrated::float < 1 then 17
         end as rating_class_pit_calibrated


 -- down 

,  case when pd_pit_calibrated_down::float >= 0 and pd_pit_calibrated_down::float < 0.0001 then 1
         when pd_pit_calibrated_down::float >= 0.0001 and pd_pit_calibrated_down::float < 0.0005 then 2
         when pd_pit_calibrated_down::float >= 0.0005 and pd_pit_calibrated_down::float < 0.0010 then 3
         when pd_pit_calibrated_down::float >= 0.0010 and pd_pit_calibrated_down::float < 0.0016 then 4
         when pd_pit_calibrated_down::float >= 0.0016 and pd_pit_calibrated_down::float <  0.0024 then 5
         when pd_pit_calibrated_down::float >= 0.0024 and pd_pit_calibrated_down::float < 0.0039 then 6
         when pd_pit_calibrated_down::float >= 0.0039 and pd_pit_calibrated_down::float < 0.0074 then 7
         when pd_pit_calibrated_down::float >= 0.0074 and pd_pit_calibrated_down::float < 0.0143 then 8
         when pd_pit_calibrated_down::float >= 0.0143 and pd_pit_calibrated_down::float < 0.0226 then 9
         when pd_pit_calibrated_down::float >= 0.0226 and pd_pit_calibrated_down::float < 0.0322 then 10
         when pd_pit_calibrated_down::float >= 0.0322 and pd_pit_calibrated_down::float < 0.0430 then 11
         when pd_pit_calibrated_down::float >= 0.0430 and pd_pit_calibrated_down::float < 0.0562 then 12
         when pd_pit_calibrated_down::float >= 0.0562 and pd_pit_calibrated_down::float < 0.1119 then 13
         when pd_pit_calibrated_down::float >= 0.1119 and pd_pit_calibrated_down::float < 0.2790 then 14
         when pd_pit_calibrated_down::float >= 0.2790 and pd_pit_calibrated_down::float < 0.4601 then 15
         when pd_pit_calibrated_down::float >= 0.4601 and pd_pit_calibrated_down::float < 0.6087 then 16
         when pd_pit_calibrated_down::float >= 0.6087 and pd_pit_calibrated_down::float < 1 then 17
         end as rating_class_pit_calibrated_down

-- neu 

,  case  when pd_pit_calibrated_neu::float >= 0 and pd_pit_calibrated_neu::float < 0.0001 then 1
         when pd_pit_calibrated_neu::float >= 0.0001 and pd_pit_calibrated_neu::float < 0.0005 then 2
         when pd_pit_calibrated_neu::float >= 0.0005 and pd_pit_calibrated_neu::float < 0.0010 then 3
         when pd_pit_calibrated_neu::float >= 0.0010 and pd_pit_calibrated_neu::float < 0.0016 then 4
         when pd_pit_calibrated_neu::float >= 0.0016 and pd_pit_calibrated_neu::float <  0.0024 then 5
         when pd_pit_calibrated_neu::float >= 0.0024 and pd_pit_calibrated_neu::float < 0.0039 then 6
         when pd_pit_calibrated_neu::float >= 0.0039 and pd_pit_calibrated_neu::float < 0.0074 then 7
         when pd_pit_calibrated_neu::float >= 0.0074 and pd_pit_calibrated_neu::float < 0.0143 then 8
         when pd_pit_calibrated_neu::float >= 0.0143 and pd_pit_calibrated_neu::float < 0.0226 then 9
         when pd_pit_calibrated_neu::float >= 0.0226 and pd_pit_calibrated_neu::float < 0.0322 then 10
         when pd_pit_calibrated_neu::float >= 0.0322 and pd_pit_calibrated_neu::float < 0.0430 then 11
         when pd_pit_calibrated_neu::float >= 0.0430 and pd_pit_calibrated_neu::float < 0.0562 then 12
         when pd_pit_calibrated_neu::float >= 0.0562 and pd_pit_calibrated_neu::float < 0.1119 then 13
         when pd_pit_calibrated_neu::float >= 0.1119 and pd_pit_calibrated_neu::float < 0.2790 then 14
         when pd_pit_calibrated_neu::float >= 0.2790 and pd_pit_calibrated_neu::float < 0.4601 then 15
         when pd_pit_calibrated_neu::float >= 0.4601 and pd_pit_calibrated_neu::float < 0.6087 then 16
         when pd_pit_calibrated_neu::float >= 0.6087 and pd_pit_calibrated_neu::float < 1 then 17
         end as rating_class_pit_calibrated_neu -- 'Unrated'

,  case  when pd_pit_calibrated_up::float >= 0 and pd_pit_calibrated_up::float < 0.0001 then 1
         when pd_pit_calibrated_up::float >= 0.0001 and pd_pit_calibrated_up::float < 0.0005 then 2
         when pd_pit_calibrated_up::float >= 0.0005 and pd_pit_calibrated_up::float < 0.0010 then 3
         when pd_pit_calibrated_up::float >= 0.0010 and pd_pit_calibrated_up::float < 0.0016 then 4
         when pd_pit_calibrated_up::float >= 0.0016 and pd_pit_calibrated_up::float <  0.0024 then 5
         when pd_pit_calibrated_up::float >= 0.0024 and pd_pit_calibrated_up::float < 0.0039 then 6
         when pd_pit_calibrated_up::float >= 0.0039 and pd_pit_calibrated_up::float < 0.0074 then 7
         when pd_pit_calibrated_up::float >= 0.0074 and pd_pit_calibrated_up::float < 0.0143 then 8
         when pd_pit_calibrated_up::float >= 0.0143 and pd_pit_calibrated_up::float < 0.0226 then 9
         when pd_pit_calibrated_up::float >= 0.0226 and pd_pit_calibrated_up::float < 0.0322 then 10
         when pd_pit_calibrated_up::float >= 0.0322 and pd_pit_calibrated_up::float < 0.0430 then 11
         when pd_pit_calibrated_up::float >= 0.0430 and pd_pit_calibrated_up::float < 0.0562 then 12
         when pd_pit_calibrated_up::float >= 0.0562 and pd_pit_calibrated_up::float < 0.1119 then 13
         when pd_pit_calibrated_up::float >= 0.1119 and pd_pit_calibrated_up::float < 0.2790 then 14
         when pd_pit_calibrated_up::float >= 0.2790 and pd_pit_calibrated_up::float < 0.4601 then 15
         when pd_pit_calibrated_up::float >= 0.4601 and pd_pit_calibrated_up::float < 0.6087 then 16
         when pd_pit_calibrated_up::float >= 0.6087 and pd_pit_calibrated_up::float < 1 then 17
         end as rating_class_pit_calibrated_up


,  case  when pd_pit_calibrated_scen_weighted::float >= 0 and pd_pit_calibrated_scen_weighted::float < 0.0001 then 1
         when pd_pit_calibrated_scen_weighted::float >= 0.0001 and pd_pit_calibrated_scen_weighted::float < 0.0005 then 2
         when pd_pit_calibrated_scen_weighted::float >= 0.0005 and pd_pit_calibrated_scen_weighted::float < 0.0010 then 3
         when pd_pit_calibrated_scen_weighted::float >= 0.0010 and pd_pit_calibrated_scen_weighted::float < 0.0016 then 4
         when pd_pit_calibrated_scen_weighted::float >= 0.0016 and pd_pit_calibrated_scen_weighted::float <  0.0024 then 5
         when pd_pit_calibrated_scen_weighted::float >= 0.0024 and pd_pit_calibrated_scen_weighted::float < 0.0039 then 6
         when pd_pit_calibrated_scen_weighted::float >= 0.0039 and pd_pit_calibrated_scen_weighted::float < 0.0074 then 7
         when pd_pit_calibrated_scen_weighted::float >= 0.0074 and pd_pit_calibrated_scen_weighted::float < 0.0143 then 8
         when pd_pit_calibrated_scen_weighted::float >= 0.0143 and pd_pit_calibrated_scen_weighted::float < 0.0226 then 9
         when pd_pit_calibrated_scen_weighted::float >= 0.0226 and pd_pit_calibrated_scen_weighted::float < 0.0322 then 10
         when pd_pit_calibrated_scen_weighted::float >= 0.0322 and pd_pit_calibrated_scen_weighted::float < 0.0430 then 11
         when pd_pit_calibrated_scen_weighted::float >= 0.0430 and pd_pit_calibrated_scen_weighted::float < 0.0562 then 12
         when pd_pit_calibrated_scen_weighted::float >= 0.0562 and pd_pit_calibrated_scen_weighted::float < 0.1119 then 13
         when pd_pit_calibrated_scen_weighted::float >= 0.1119 and pd_pit_calibrated_scen_weighted::float < 0.2790 then 14
         when pd_pit_calibrated_scen_weighted::float >= 0.2790 and pd_pit_calibrated_scen_weighted::float < 0.4601 then 15
         when pd_pit_calibrated_scen_weighted::float >= 0.4601 and pd_pit_calibrated_scen_weighted::float < 0.6087 then 16
         when pd_pit_calibrated_scen_weighted::float >= 0.6087 and pd_pit_calibrated_scen_weighted::float < 1 then 17
         end as rating_class_pit_calibrated_scen_weighted


-- Final PD and rating (PIT)

, case  when d.user_id is not null or ins.user_id is not null or sch.user_id is not null then 1
        when dpd > 90 then 1
        when max(case when product_flag in ('RP_2') then 1 else 0 end) over(partition by c.user_id) = 1 then
              case when dpd > 90 then 1
                  when dpd > 60 then  0.7000
                  when dpd > 30 then 0.5292
                  when dpd > 0 then 0.4000
                  else pd_pit_calibrated
               end 
        when pd_pit_calibrated is not null then pd_pit_calibrated
        when pd_pit_calibrated is null then 
             case when dpd > 90 then 1
                  when dpd > 60 then  0.7000
                  when dpd > 30 then 0.5292
                  when dpd > 0 then 0.4000
                  else 
                     case when schr.rating = 'A' then 0.0049
                          when schr.rating = 'B' then 0.0110
                          when schr.rating = 'C' then 0.0185
                          when schr.rating = 'D' then 0.0275
                          when schr.rating = 'E' then 0.0375
                          when schr.rating = 'F' then 0.0491
                          when schr.rating = 'G' then 0.0643
                          when schr.rating in ('H','I','K','L', 'M','UNKNOWN') then 0.1945
                          when schr.rating = 'N' then 0.4000
                          when schr.rating = 'O' then 0.5292
                          when schr.rating = 'P' then 0.7000
                          else 0.4
                          end
                    end
       else 0.4 end as PD

-- PD scenarios 

-- down

, case  when d.user_id is not null or ins.user_id is not null or sch.user_id is not null then 1
        when dpd > 90 then 1
        when max(case when product_flag in ('RP_2') then 1 else 0 end) over(partition by c.user_id) = 1 then
              case when dpd > 90 then 1
                  when dpd > 60 then  0.7000
                  when dpd > 30 then 0.5292
                  when dpd > 0 then 0.4000
                  else pd_pit_calibrated_down
               end  
        when pd_pit_calibrated_down is not null then pd_pit_calibrated_down
        when pd_pit_calibrated_down is null then 
             case when dpd > 90 then 1
                  when dpd > 60 then  0.7000
                  when dpd > 30 then 0.5292
                  when dpd > 0 then 0.4000
                  else 
                     case when schr.rating = 'A' then 0.0049
                          when schr.rating = 'B' then 0.0110
                          when schr.rating = 'C' then 0.0185
                          when schr.rating = 'D' then 0.0275
                          when schr.rating = 'E' then 0.0375
                          when schr.rating = 'F' then 0.0491
                          when schr.rating = 'G' then 0.0643
                          when schr.rating in ('H','I','K','L', 'M','UNKNOWN') then 0.1945
                          when schr.rating = 'N' then 0.4000
                          when schr.rating = 'O' then 0.5292
                          when schr.rating = 'P' then 0.7000
                          else 0.4
                          end
                    end
       else 0.4 end as PD_down

-- neu

, case  when d.user_id is not null or ins.user_id is not null or sch.user_id is not null then 1
        when dpd > 90 then 1
        when max(case when product_flag in ('RP_2') then 1 else 0 end) over(partition by c.user_id) = 1 then
              case when dpd > 90 then 1
                  when dpd > 60 then  0.7000
                  when dpd > 30 then 0.5292
                  when dpd > 0 then 0.4000
                  else pd_pit_calibrated_neu
               end 
        when pd_pit_calibrated_neu is not null then pd_pit_calibrated_neu
        when pd_pit_calibrated_neu is null then 
             case when dpd > 90 then 1
                  when dpd > 60 then  0.7000
                  when dpd > 30 then 0.5292
                  when dpd > 0 then 0.4000
                  else 
                     case when schr.rating = 'A' then 0.0049
                          when schr.rating = 'B' then 0.0110
                          when schr.rating = 'C' then 0.0185
                          when schr.rating = 'D' then 0.0275
                          when schr.rating = 'E' then 0.0375
                          when schr.rating = 'F' then 0.0491
                          when schr.rating = 'G' then 0.0643
                          when schr.rating in ('H','I','K','L', 'M','UNKNOWN') then 0.1945
                          when schr.rating = 'N' then 0.4000
                          when schr.rating = 'O' then 0.5292
                          when schr.rating = 'P' then 0.7000
                          else 0.4
                          end
                    end
       else 0.4 end as PD_neu

-- up 

, case  when d.user_id is not null or ins.user_id is not null or sch.user_id is not null then 1
        when dpd > 90 then 1
        when max(case when product_flag in ('RP_2') then 1 else 0 end) over(partition by c.user_id) = 1 then
              case when dpd > 90 then 1
                  when dpd > 60 then  0.7000
                  when dpd > 30 then 0.5292
                  when dpd > 0 then 0.4000
                  -- when pd_pit_calibrated_up < 0.125 then pd_pit_calibrated_up
                  else pd_pit_calibrated_up
               end 
        when pd_pit_calibrated_up is not null then pd_pit_calibrated_up
        when pd_pit_calibrated_up is null then 
             case when dpd > 90 then 1
                  when dpd > 60 then  0.7000
                  when dpd > 30 then 0.5292
                  when dpd > 0 then 0.4000
                  else 
                     case when schr.rating = 'A' then 0.0049
                          when schr.rating = 'B' then 0.0110
                          when schr.rating = 'C' then 0.0185
                          when schr.rating = 'D' then 0.0275
                          when schr.rating = 'E' then 0.0375
                          when schr.rating = 'F' then 0.0491
                          when schr.rating = 'G' then 0.0643
                          when schr.rating in ('H','I','K','L', 'M','UNKNOWN') then 0.1945
                          when schr.rating = 'N' then 0.4000
                          when schr.rating = 'O' then 0.5292
                          when schr.rating = 'P' then 0.7000
                          else 0.4
                          end
                    end
       else 0.4 end as PD_up


, 2.5*PD_down/100 + 95*PD_neu/100 + 2.5*PD_up/100 as PD_scen_weighted

from {{ ref('bp_portfolio_customers_aud_m_v2') }} c 
left join {{ ref('bp_portfolio_balance_aud_m_v2') }} o on o.encoded_key = c.encoded_key and c.reporting_date::date = o.reporting_date::date
left join {{ ref('bp_application_pd_supermodel_m_v2') }} pd_app on pd_app.encoded_key = c.encoded_key 
                                                                and pd_app.reporting_date::date = c.reporting_date::date
left join {{ ref('bp_overdraft_split_m_v2') }}  mll on mll.encoded_key = c.encoded_key and c.reporting_date::date = mll.reporting_date::date
left join {{ ref('bp_monitoring_pd_supermodel_v2_m_v2') }} m on m.user_id = c.user_id and m.reporting_date::date = c.reporting_date::date
-- to be fixed - application scores should be from porto
left join {{ ref('bp_default_dunning_m_v2') }} d on d.user_id = c.user_id  and d.reporting_date::date = c.reporting_date::date   
left join {{ ref('bp_default_internal_insolvency_v2_m_v2') }} ins on ins.user_id = c.user_id and ins.reporting_date::date = c.reporting_date::date
left join {{ ref('bp_default_credit_bureau_insolvency_v3_m_v2') }} sch on sch.user_id = c.user_id and sch.reporting_date::date = c.reporting_date::date
left join {{ ref('bp_repayment_plan_ph1_m_v2') }} rp on rp.user_id = c.user_id and rp.reporting_date::date = c.reporting_date::date
left join {{ ref('bp_credit_bureau_rating_m_v2') }} schr on schr.user_id = c.user_id and schr.reporting_date::date = c.reporting_date::date
left join dbt.bp_arrears_reg_aud de on de.user_id = c.user_id
                                       and  last_day(date_add('month', -1, last_day(getdate()))) between start_in_arrears::date and end_in_arrears::date
left join {{ ref('bp_overdraft_limit_periods_v2') }} p on c.encoded_key = p.encoded_key 
                                                        and p.amount_cents > 0 
                                                        and date_add('minute', -1, date_add('day', 1, last_day(date_add('month', -1, last_day(getdate())))) ) between p.rev_timestamp and p.end_timestamp
left join first_enabled_date f on f.encoded_key = c.encoded_key and f.user_id = c.user_id and f.first_creation_date::date <= last_day(date_add('month', -1, last_day(getdate())))
left join repayment r on r.parent_account_key = c.encoded_key
left join  {{ source('dbt', 'zrh_users') }} z on z.user_id = c.user_id 
left join {{ ref('bp_loan_contract_information_m_v2') }} li on li.encoded_key = c.encoded_key and li.reporting_date::date = c.reporting_date::date
left join {{ source('public', 'mmbr_savings_account') }} s on s.encoded_key = c.encoded_key and s.account_type = 'CURRENT_ACCOUNT'
where 
c.reporting_date::date = last_day(date_add('month', -1, last_day(getdate())))
and case when z.closed_at <= last_day(date_add('month', -1, last_day(getdate()))) 
                and coalesce(o.balance,0) = 0 then 1 
           when c.encoded_key is null then 1
      else 0 end = 0
order by 1
)

select * 
, getdate() as etl_updated
, coalesce(instrument_id, '') || coalesce(etl_updated::varchar, '') as unique_key
from total 
{% if is_incremental() %}
where etl_updated > (select max(etl_updated) from {{this}})
{% endif %}

