

-- dbt run --full-refresh --select bp_retail_provisions_stg_v5_v4_updated_v2 (first time)
-- dbt run --select bp_retail_provisions_stg_v5_v4_updated_v2 (other runs)

with 

first_enabled_date as (
select user_id 
, encoded_key
, min(rev_timestamp) as first_creation_date
, max(rev_timestamp) as last_creation_date
from "n26"."credit_risk_playground"."bp_overdraft_limit_periods_v2"
where rev_timestamp <= date_add('minute', -1, date_add('day', 1, last_day(date_add('month', -1, last_day(getdate())))) ) and amount_cents > 0 
group by 
user_id 
, encoded_key
)

, repayment as (
select parent_account_key
, min(due_date) as first_repayment
, max(due_date) as last_repayment
from  "n26"."public"."mmbr_repayment"  
group by 1
)

, forbearance_last_year as (
select distinct 
instrument_id
from "n26"."credit_risk_playground"."bp_forbearance_m_v2_v2" f 
where f.credit_creation_date::date between date_add('month', -12
                                                    , last_day(date_add('month', -1, last_day(getdate())))
                                                    ) and last_day(date_add('month', -1, last_day(getdate())))
      and f.reportdate = last_day(date_add('month', -1, last_day(getdate())))

)

, latest_default_during_forbearance as (
select 
c.user_id 
--, c.instrument_id 
, max(c.reportdate::date) as default_during_forbearance_date
from "n26"."credit_risk_playground"."bp_retail_provisions_stg_v5_pre_updated_v2" c 
left join "n26"."credit_risk_playground"."bp_forbearance_m_v2_v2" f on c.instrument_id = f.instrument_id and c.reportdate::date = f.reportdate::date
where (c.is_default = 1) and (f.instrument_id is not null)
group by 1
)

, default_last_12_months as (
select 
c.user_id 
--, c.instrument_id 
, max(c.reportdate::date) as last_default_date
from "n26"."credit_risk_playground"."bp_retail_provisions_stg_v5_pre_updated_v2" c 
where (c.is_default = 1) 
group by 1
)

, total as (
select distinct

-- 1. Customers base

c.user_id 
, c.Instrument_ID
, c.Country
, c.LegalEntity
, c.IS_IFRS -- "No" for TSY bonds and "Yes" for other instruments
, c.ReportDate
, c.CalcDate -- for retail same as eom, for mortgage 1-month lag
, c.Script_calculation_date
-- 2. Product flag
-- product

, c.product_flag

, c.Instrument_Type

, c.Instrument 

-- 3. Creation date 

, c.credit_creation_date

-- 4. DPD

, c.start_in_arrears
, c.dpd

-- 5. Default

, c.insolvency 
, c.dunning 
, c.schufa_insolvency
, c.rep_plan_user

, c.is_default


-- 6. Credit Bureau flags

, c.schufa_rating 
, c.provider
, c.schufa_rating_date

-- 7. Limit

, c."Limit"

, c.last_limit_eur

, c.last_limit_date

-- 8. Exposures

, c.TBIL_exposures

, c.CC_exposures

, c.RP_2_exposures

, c.OD_exposures
-- 9. PD

-- Calibration segment

-- ! Important ! In Porto if customer eligible for different calibration segment, we choose the one with the max PD

, c.porto_calibration_segment

, c.pit_calibration_segment  

-- Available Porto PD

, c.internal_pd

,  c.logit

, c.pd_pit_calibrated

-- MEV scenarios

-- down 

, c.pd_pit_calibrated_down

-- neu

, c.pd_pit_calibrated_neu

-- up

, c.pd_pit_calibrated_up

, c.pd_pit_calibrated_scen_weighted

-- Calibration PD

,  c.rating_class_pit_calibrated


 -- down 

,  c.rating_class_pit_calibrated_down

-- neu 

,  c.rating_class_pit_calibrated_neu

,  c.rating_class_pit_calibrated_up


,  c.rating_class_pit_calibrated_scen_weighted


-- Final PD and rating (PIT)

, case when (df.user_id is not null)
            and (c.reportdate::date between df.default_during_forbearance_date::date and date_add('month', 12, df.default_during_forbearance_date::date)) then 1 
       else c.PD end as PD

-- PD scenarios 

-- down

, case when (df.user_id is not null)
            and (c.reportdate::date between df.default_during_forbearance_date::date and date_add('month', 12, df.default_during_forbearance_date::date)) then 1 
       else c.PD_down end as PD_down

-- neu

, case when (df.user_id is not null)
            and (c.reportdate::date between df.default_during_forbearance_date::date and date_add('month', 12, df.default_during_forbearance_date::date)) then 1 
       else c.PD_neu end as PD_neu

-- up 

, case when (df.user_id is not null)
            and (c.reportdate::date between df.default_during_forbearance_date::date and date_add('month', 12, df.default_during_forbearance_date::date)) then 1 
       else c.PD_up end as PD_up


, case when (df.user_id is not null)
            and (c.reportdate::date between df.default_during_forbearance_date::date and date_add('month', 12, df.default_during_forbearance_date::date)) then 1 
       else c.PD_scen_weighted end as PD_scen_weighted


-- Final Rating

, case when d.user_id is not null or ins.user_id is not null or sch.user_id is not null then '19'
       when (df.user_id is not null)
            and (c.reportdate::date between df.default_during_forbearance_date::date and date_add('month', 12, df.default_during_forbearance_date::date)) then '19' 
       when c.dpd > 90 then '18'
       when max(case when c.product_flag in ('RP_2') then 1 else 0 end) over(partition by c.user_id) = 1 then
              case when c.dpd > 90 then '18'
                  when c.dpd > 60 then  '17'
                  when c.dpd > 30 then '16'
                  when c.dpd > 0 then '15'
                  else cast(c.rating_class_pit_calibrated as varchar)
               end 
       when c.pd_pit_calibrated is not null then cast(c.rating_class_pit_calibrated as varchar)
       when c.pd_pit_calibrated is null then 
            case when c.dpd > 90 then '18'
                 when c.dpd > 60 then '17'
                 when c.dpd > 30 then '16'
                 when c.dpd > 0 then '15'
                 else 
                     case when schr.rating = 'A' then '7'
                          when schr.rating = 'B' then '8'
                          when schr.rating = 'C' then '9'
                          when schr.rating = 'D' then '10'
                          when schr.rating = 'E' then '11'
                          when schr.rating = 'F' then '12'
                          when schr.rating = 'G' then '13'
                          when schr.rating in ('H','I','K','L', 'M','UNKNOWN') then '14'
                          when schr.rating = 'N' then '15'
                          when schr.rating = 'O' then '16'
                          when schr.rating = 'P' then '17'
                          else 'unrated'
                          end
                    end
       else 'unrated' end as CurrentRating

-- Final Rating scenarios 

-- down

, case when d.user_id is not null or ins.user_id is not null or sch.user_id is not null then '19'
when (df.user_id is not null)
            and (c.reportdate::date between df.default_during_forbearance_date::date and date_add('month', 12, df.default_during_forbearance_date::date)) then '19'
       when c.dpd > 90 then '18'
       when max(case when c.product_flag in ('RP_2') then 1 else 0 end) over(partition by c.user_id) = 1 then
              case when c.dpd > 90 then '18'
                  when c.dpd > 60 then  '17'
                  when c.dpd > 30 then '16'
                  when c.dpd > 0 then '15'
                  else cast(c.rating_class_pit_calibrated_down as varchar)
               end 
         when c.pd_pit_calibrated_down is not null then cast(c.rating_class_pit_calibrated_down as varchar)
         when c.pd_pit_calibrated_down is null then 
            case when c.dpd > 90 then '18'
                 when c.dpd > 60 then '17'
                 when c.dpd > 30 then '16'
                 when c.dpd > 0 then '15'
                 else 
                     case when schr.rating = 'A' then '7'
                          when schr.rating = 'B' then '8'
                          when schr.rating = 'C' then '9'
                          when schr.rating = 'D' then '10'
                          when schr.rating = 'E' then '11'
                          when schr.rating = 'F' then '12'
                          when schr.rating = 'G' then '13'
                          when schr.rating in ('H','I','K','L', 'M','UNKNOWN') then '14'
                          when schr.rating = 'N' then '15'
                          when schr.rating = 'O' then '16'
                          when schr.rating = 'P' then '17'
                          else 'unrated'
                          end
                    end
       else 'unrated' end as CurrentRating_down

-- neu

, case when d.user_id is not null or ins.user_id is not null or sch.user_id is not null then '19'
       when (df.user_id is not null)
            and (c.reportdate::date between df.default_during_forbearance_date::date and date_add('month', 12, df.default_during_forbearance_date::date)) then '19'
       when c.dpd > 90 then '18'
       when max(case when c.product_flag in ('RP_2') then 1 else 0 end) over(partition by c.user_id) = 1 then
              case when c.dpd > 90 then '18'
                  when c.dpd > 60 then  '17'
                  when c.dpd > 30 then '16'
                  when c.dpd > 0 then '15'
                  else cast(c.rating_class_pit_calibrated_neu as varchar)
               end 
         when c.pd_pit_calibrated_neu is not null then cast(c.rating_class_pit_calibrated_neu as varchar)
         when c.pd_pit_calibrated_neu is null then 
            case when c.dpd > 90 then '18'
                 when c.dpd > 60 then '17'
                 when c.dpd > 30 then '16'
                 when c.dpd > 0 then '15'
                 else 
                     case when schr.rating = 'A' then '7'
                          when schr.rating = 'B' then '8'
                          when schr.rating = 'C' then '9'
                          when schr.rating = 'D' then '10'
                          when schr.rating = 'E' then '11'
                          when schr.rating = 'F' then '12'
                          when schr.rating = 'G' then '13'
                          when schr.rating in ('H','I','K','L', 'M','UNKNOWN') then '14'
                          when schr.rating = 'N' then '15'
                          when schr.rating = 'O' then '16'
                          when schr.rating = 'P' then '17'
                          else 'unrated'
                          end
                    end
       else 'unrated' end as CurrentRating_neu


-- up 

, case when d.user_id is not null or ins.user_id is not null or sch.user_id is not null then '19'
       when (df.user_id is not null)
            and (c.reportdate::date between df.default_during_forbearance_date::date and date_add('month', 12, df.default_during_forbearance_date::date)) then '19'
       when c.dpd > 90 then '18'
       when max(case when c.product_flag in ('RP_2') then 1 else 0 end) over(partition by c.user_id) = 1 then
              case when c.dpd > 90 then '18'
                  when c.dpd > 60 then  '17'
                  when c.dpd > 30 then '16'
                  when c.dpd > 0 then '15'
                  else cast(c.rating_class_pit_calibrated_up as varchar)
               end 
         when c.pd_pit_calibrated_up is not null then cast(c.rating_class_pit_calibrated_up as varchar)
         when c.pd_pit_calibrated_up is null then 
            case when c.dpd > 90 then '18'
                 when c.dpd > 60 then '17'
                 when c.dpd > 30 then '16'
                 when c.dpd > 0 then '15'
                 else 
                     case when schr.rating = 'A' then '7'
                          when schr.rating = 'B' then '8'
                          when schr.rating = 'C' then '9'
                          when schr.rating = 'D' then '10'
                          when schr.rating = 'E' then '11'
                          when schr.rating = 'F' then '12'
                          when schr.rating = 'G' then '13'
                          when schr.rating in ('H','I','K','L', 'M','UNKNOWN') then '14'
                          when schr.rating = 'N' then '15'
                          when schr.rating = 'O' then '16'
                          when schr.rating = 'P' then '17'
                          else 'unrated'
                          end
                    end
       else 'unrated' end as CurrentRating_up

-- scneraios-weighted 

, case when d.user_id is not null or ins.user_id is not null or sch.user_id is not null then '19'
       when (df.user_id is not null)
            and (c.reportdate::date between df.default_during_forbearance_date::date and date_add('month', 12, df.default_during_forbearance_date::date)) then '19'
       when c.dpd > 90 then '18'
       when max(case when c.product_flag in ('RP_2') then 1 else 0 end) over(partition by c.user_id) = 1 then
              case when c.dpd > 90 then '18'
                  when c.dpd > 60 then  '17'
                  when c.dpd > 30 then '16'
                  when c.dpd > 0 then '15'
                  else cast(c.rating_class_pit_calibrated_scen_weighted as varchar)
               end 
         when c.pd_pit_calibrated_scen_weighted is not null then cast(c.rating_class_pit_calibrated_scen_weighted as varchar)
         when c.pd_pit_calibrated_scen_weighted is null then 
            case when c.dpd > 90 then '18'
                 when c.dpd > 60 then '17'
                 when c.dpd > 30 then '16'
                 when c.dpd > 0 then '15'
                 else 
                     case when schr.rating = 'A' then '7'
                          when schr.rating = 'B' then '8'
                          when schr.rating = 'C' then '9'
                          when schr.rating = 'D' then '10'
                          when schr.rating = 'E' then '11'
                          when schr.rating = 'F' then '12'
                          when schr.rating = 'G' then '13'
                          when schr.rating in ('H','I','K','L', 'M','UNKNOWN') then '14'
                          when schr.rating = 'N' then '15'
                          when schr.rating = 'O' then '16'
                          when schr.rating = 'P' then '17'
                          else 'unrated' -- 'Unrated'
                          end
                    end
       else 'unrated' end as CurrentRating_scen_weighted

-- Final Rating date

, case when d.user_id is not null or ins.user_id is not null or sch.user_id is not null then last_day(date_add('month', -1, last_day(getdate())))
       when (df.user_id is not null)
            and (c.reportdate::date between df.default_during_forbearance_date::date and date_add('month', 12, df.default_during_forbearance_date::date)) then last_day(date_add('month', -1, last_day(getdate())))
       when c.dpd > 90 then last_day(date_add('month', -1, last_day(getdate())))
       when max(case when c.product_flag in ('RP_2') then 1 else 0 end) over(partition by c.user_id) = 1 then
              case when c.dpd > 90 then last_day(date_add('month', -1, last_day(getdate())))
                  when c.dpd > 60 then  last_day(date_add('month', -1, last_day(getdate())))
                  when c.dpd > 30 then last_day(date_add('month', -1, last_day(getdate())))
                  when c.dpd > 0 then last_day(date_add('month', -1, last_day(getdate())))
                  --when c.rating_class_pit_calibrated < 14 then m.calculated_at::date 
                  else last_day(date_add('month', -1, last_day(getdate())))
               end 
       when c.pd_pit_calibrated is not null then m.calculated_at::date 
       when c.pd_pit_calibrated is null then 
            case when c.dpd > 90 then last_day(date_add('month', -1, last_day(getdate())))
                 when c.dpd > 60 then last_day(date_add('month', -1, last_day(getdate())))
                 when c.dpd > 30 then last_day(date_add('month', -1, last_day(getdate())))
                 when c.dpd > 0 then last_day(date_add('month', -1, last_day(getdate())))
                 else 
                     case when schr.rating = 'A' then last_day(date_add('month', -1, last_day(getdate())))
                          when schr.rating = 'B' then last_day(date_add('month', -1, last_day(getdate())))
                          when schr.rating = 'C' then last_day(date_add('month', -1, last_day(getdate())))
                          when schr.rating = 'D' then last_day(date_add('month', -1, last_day(getdate())))
                          when schr.rating = 'E' then last_day(date_add('month', -1, last_day(getdate())))
                          when schr.rating = 'F' then last_day(date_add('month', -1, last_day(getdate())))
                          when schr.rating = 'G' then last_day(date_add('month', -1, last_day(getdate())))
                          when schr.rating in ('H','I','K','L', 'M','UNKNOWN') then last_day(date_add('month', -1, last_day(getdate())))
                          when schr.rating = 'N' then last_day(date_add('month', -1, last_day(getdate())))
                          when schr.rating = 'O' then last_day(date_add('month', -1, last_day(getdate())))
                          when schr.rating = 'P' then last_day(date_add('month', -1, last_day(getdate())))
                          else last_day(date_add('month', -1, last_day(getdate())))
                          end
                    end
       else last_day(date_add('month', -1, last_day(getdate())))
       end as CurrentRating_date

-- Final rating source

, case when d.user_id is not null or ins.user_id is not null or sch.user_id is not null then 'default_flag'
       when (df.user_id is not null)
            and (c.reportdate::date between df.default_during_forbearance_date::date and date_add('month', 12, df.default_during_forbearance_date::date)) then 'default_flag_fbe'
       when c.dpd > 90 then 'default_flag'
       when max(case when c.product_flag in ('RP_2') then 1 else 0 end) over(partition by c.user_id) = 1 then
              case when c.dpd > 90 then 'default_flag'
                  when c.dpd > 60 then  'dpd 61-90'
                  when c.dpd > 30 then 'dpd 31-60'
                  when c.dpd > 0 then 'dpd 1-30'
                  else 'repayment_plan_2'
               end 
         when c.pd_pit_calibrated_scen_weighted is not null then 'internal_rating_model'
         when c.pd_pit_calibrated_scen_weighted is null then 
            case when c.dpd > 90 then 'default_flag'
                 when c.dpd > 60 then 'dpd 61-90'
                 when c.dpd > 30 then 'dpd 31-60'
                 when c.dpd > 0 then 'dpd 1-30'
                 else 
                     case when schr.rating = 'A' then 'CB'
                          when schr.rating = 'B' then 'CB'
                          when schr.rating = 'C' then 'CB'
                          when schr.rating = 'D' then 'CB'
                          when schr.rating = 'E' then 'CB'
                          when schr.rating = 'F' then 'CB'
                          when schr.rating = 'G' then 'CB'
                          when schr.rating in ('H','I','K','L', 'M','UNKNOWN') then 'CB'
                          when schr.rating = 'N' then 'CB'
                          when schr.rating = 'O' then 'CB'
                          when schr.rating = 'P' then 'CB'
                          else 'no_rating'
                          end
                    end
       else 'no_rating' end as CurrentRating_source

 -- 10. CCF

, case when coalesce(c."Limit"::numeric,0) = 0 then 0
            when coalesce(c.OD_exposures::numeric,0) = 0 then 0
            else round(coalesce(c.OD_exposures::numeric,0)/coalesce(c."Limit"::numeric,0), 2) 
        end as utilization_pct
, case
            when coalesce(c.OD_exposures,0)  > coalesce(c."Limit",0) then ' > 80%'
            when utilization_pct < 0.2 then '  <=20%'
            when utilization_pct between 0.2 and 0.4 then ' 20% to 40%'
            when utilization_pct between 0.4 and 0.6 then ' 40% to 60%'
            when utilization_pct between 0.6 and 0.8 then ' 60% to 80%'
            when utilization_pct >= 0.8 then ' > 80%' 
        end as usage

, decode(usage,
            '  <=20%', 0.3644,
            ' 20% to 40%', 0.6360,
            ' 40% to 60%', 0.7221,
            ' 60% to 80%', 0.8089,
            ' > 80%', 1) as ccf

, case when CurrentRating not in ('18', '19') 
            and c.product_flag in ('RP_1', 'Arranged Overdraft') then  

                case when coalesce(c."Limit",0) <= coalesce(c.OD_exposures,0) then 0
                     when usage not in (' > 80%') then (coalesce(c."Limit",0) -  coalesce(c.OD_exposures,0)) 
                     else 0
                     end 

        else 0 end off_balance_exposure

-- 11. EAD

, case when CurrentRating not in ('18', '19') 
            and c.product_flag in ('RP_1', 'Arranged Overdraft') then  
                case when coalesce(c."Limit",0) <= coalesce(c.OD_exposures,0) then 0
                     when usage not in (' > 80%') then ((coalesce(c."Limit",0) -  coalesce(c.OD_exposures,0)) * ccf )
                     else 0
                     end 
        else 0 end OffBalance_EAD -- only for off-balance exposures by finance request

, coalesce(o.balance,0) as OnBalance_EAD

, case when OffBalance_EAD <> 0 then 1 else 0 end as IsOffBalance
-- 12. LGD 

, CASE
    WHEN c.is_default = 1 
         AND GREATEST(
                COALESCE(c.credit_creation_date, DATE '0001-01-01'),
                COALESCE(
                    CASE 
                        WHEN d.user_id IS NOT NULL THEN d.default_date
                        WHEN ins.user_id IS NOT NULL THEN ins.internal_insolvency_date
                        WHEN sch.user_id IS NOT NULL THEN c.schufa_rating_date
                        WHEN c.dpd > 90 THEN date_add('day', 90, c.start_in_arrears)
                    END,
                    DATE '0001-01-01'
                )
             ) < date_add('year', -3, c.reportdate)
    THEN 1.000000::float
  else
     case 
       when c.product_flag in ('Arranged Overdraft', 'RP_1', 'RP_2') and sum(case when c.product_flag in ('Arranged Overdraft', 'RP_1', 'RP_2') then coalesce(o.balance,0) else 0 end) over(partition by c.user_id) < 500.22 then 0.461767::float
       when c.product_flag in ('Arranged Overdraft', 'RP_1', 'RP_2') and sum(case when c.product_flag in ('Arranged Overdraft', 'RP_1', 'RP_2') then coalesce(o.balance,0) else 0 end) over(partition by c.user_id) < 2751.38 then 0.521929::float
       when c.product_flag in ('Arranged Overdraft', 'RP_1', 'RP_2') and sum(case when c.product_flag in ('Arranged Overdraft', 'RP_1', 'RP_2') then coalesce(o.balance,0) else 0 end) over(partition by c.user_id) < 5992.30 then 0.552278::float
       when c.product_flag in ('Arranged Overdraft', 'RP_1', 'RP_2') and sum(case when c.product_flag in ('Arranged Overdraft', 'RP_1', 'RP_2') then coalesce(o.balance,0) else 0 end) over(partition by c.user_id) >= 5992.30 then 0.71094::float
       when c.product_flag in ('TBIL') and country = 'DEU' and sum(case when c.product_flag in ('TBIL') and country = 'DEU' then coalesce(o.balance,0) else 0 end) over(partition by c.user_id) < 102.50 then 0.287018::float
       when c.product_flag in ('TBIL') and country = 'DEU' and sum(case when c.product_flag in ('TBIL') and country = 'DEU' then coalesce(o.balance,0) else 0 end) over(partition by c.user_id) < 240.50 then 0.368662::float
       when c.product_flag in ('TBIL') and country = 'DEU' and sum(case when c.product_flag in ('TBIL') and country = 'DEU' then coalesce(o.balance,0) else 0 end) over(partition by c.user_id) < 526.00 then 0.443474::float
       when c.product_flag in ('TBIL') and country = 'DEU' and sum(case when c.product_flag in ('TBIL') and country = 'DEU' then coalesce(o.balance,0) else 0 end) over(partition by c.user_id) >= 526.00 then 0.520718::float
       when c.product_flag in ('TBIL') and country = 'FRA' and sum(case when c.product_flag in ('TBIL') and country = 'FRA' then coalesce(o.balance,0) else 0 end) over(partition by c.user_id) < 43.50 then 0.840663::float
       when c.product_flag in ('TBIL') and country = 'FRA' and sum(case when c.product_flag in ('TBIL') and country = 'FRA' then coalesce(o.balance,0) else 0 end) over(partition by c.user_id) < 83.50 then 0.846696::float
       when c.product_flag in ('TBIL') and country = 'FRA' and sum(case when c.product_flag in ('TBIL') and country = 'FRA' then coalesce(o.balance,0) else 0 end) over(partition by c.user_id) < 123.50 then 0.878149::float
       when c.product_flag in ('TBIL') and country = 'FRA' and sum(case when c.product_flag in ('TBIL') and country = 'FRA' then coalesce(o.balance,0) else 0 end) over(partition by c.user_id) >= 123.50 then 0.924742::float
       when c.product_flag in ('TBIL') and country in ('ESP', 'ITA') and sum(case when c.product_flag in ('TBIL') and country in ('ESP', 'ITA') then coalesce(o.balance,0) else 0 end) over(partition by c.user_id) < 61.50 then 0.865395::float
       when c.product_flag in ('TBIL') and country in ('ESP', 'ITA') and sum(case when c.product_flag in ('TBIL') and country in ('ESP', 'ITA') then coalesce(o.balance,0) else 0 end) over(partition by c.user_id) < 124.00 then 0.90235::float
       when c.product_flag in ('TBIL') and country in ('ESP', 'ITA') and sum(case when c.product_flag in ('TBIL') and country in ('ESP', 'ITA') then coalesce(o.balance,0) else 0 end) over(partition by c.user_id) >= 124.00 then 0.941614::float
       when c.product_flag = 'CC' then 0.678207::float
       when c.product_flag = 'TBIL' then 0.287018::float
       when c.product_flag in ('Unarranged Overdraft') and country in ('AUT') then 0.818600::float
       when c.product_flag in ('Unarranged Overdraft') and country in ('BEL') then 0.842100::float
       when c.product_flag in ('Unarranged Overdraft') and country in ('DEU') then 0.776100::float
       when c.product_flag in ('Unarranged Overdraft') and country in ('ESP') then 0.931700::float
       when c.product_flag in ('Unarranged Overdraft') and country in ('FRA') then 0.870900::float
       when c.product_flag in ('Unarranged Overdraft') and country in ('IRL') then 0.704100::float
       when c.product_flag in ('Unarranged Overdraft') and country in ('ITA') then 0.843200::float
       when c.product_flag in ('Unarranged Overdraft') and country in ('NLD') then 0.773000::float
       when c.product_flag in ('Unarranged Overdraft') and country in ('POL') then 0.926300::float
       when c.product_flag in ('Unarranged Overdraft') and country in ('PRT') then 0.837700::float
       when c.product_flag in ('Unarranged Overdraft') then 0.904800::float
     else 0.800000::float
     end
end as LGD

-- 13. Finance

, s.overdraft_interest_accrued
-- , mi.interest_rate as interest_rate_overdraft
, li.interest_rate_loan
, li.accrued_interest_loan
, r.first_repayment
, r.last_repayment

, li.loan_amount 
, li.loan_name
, li.repayment_installments
, li.repayment_period_count
, li.principal_repayment_interval

-- 14. Maturity

,  r.last_repayment::date as MaturityDate

, case when c.product_flag in ('RP_1', 'Arranged Overdraft', 'Unarranged Overdraft') then 12
       when r.last_repayment::date < last_day(date_add('month', -1, last_day(getdate()))) then 12 -- edge case, late customers payments
       when round(datediff('month', last_day(date_add('month', -1, last_day(getdate()))), r.last_repayment::date)) = 0 then 1 -- edge case, less than 1 month till maturity
       else round(datediff('month', last_day(date_add('month', -1, last_day(getdate()))), r.last_repayment::date)) end as Maturity

-- 15. Stages 

-- for stage 2 reasons in https://docs.google.com/document/d/1xnfIHGd-4GTxUJ2QiZ04ImMk8yYe2EfBctrk3dW-Bjc/edit# : Section 5, only with Intensified Management
 --flag
 --and +200% PD increase as significat risk increase measure
 

-- application_date

, case when c.product_flag in ('Arranged Overdraft', 'RP_1') then coalesce(f.first_creation_date::date,last_day(date_add('month', -1, last_day(getdate()))))
       when c.product_flag <> 'Unarranged Overdraft' then coalesce(li.creation_date::date,last_day(date_add('month', -1, last_day(getdate()))))
       else null end as InitialPD_date
       
, case when coalesce(
              coalesce(
                coalesce(pd_sc.pd_init::float
                           , pd_od.pd_init::float)
                              , pd_app.pd_2::float)
                                , pd_app_ad.pd_2::float) is null and c.product_flag = 'RP_2' then c.pd_pit_calibrated_scen_weighted::float
       else coalesce(
              coalesce(
                coalesce(pd_sc.pd_init::float
                           , pd_od.pd_init::float)
                              , pd_app.pd_2::float)
                                , pd_app_ad.pd_2::float) end as InitialPD

, case when coalesce(
              coalesce(
                coalesce(pd_sc.pd_init::float
                           , pd_od.pd_init::float)
                              , pd_app.pd_2::float)
                                , pd_app_ad.pd_2::float) is null and c.product_flag = 'RP_2' then 'repayment_plan_2'
       else case when pd_sc.pd_init::float is not null then 'manually_scored_loans_sample'
                 when pd_od.pd_init::float is not null then 'manually_scored_overdraft_sample'
                 when pd_app.pd_2::float is not null then 'application_score_porto'
                 when pd_app_ad.pd_2::float is not null then 'application_score_porto'
                 end 

       end InitialPD_source

, (1.0 - pow(1.0 - case when coalesce(
                                coalesce(
                                   coalesce(pd_sc.pd_init::float
                                          , pd_od.pd_init::float)
                                                 , pd_app.pd_2::float)
                                                 , pd_app_ad.pd_2::float) is null and c.product_flag = 'RP_2' then c.pd_pit_calibrated_scen_weighted::float
                            else coalesce(
                                   coalesce(
                                          coalesce(pd_sc.pd_init::float
                                                 , pd_od.pd_init::float)
                                                        , pd_app.pd_2::float)
                                                        , pd_app_ad.pd_2::float) end, case when c.product_flag in ('RP_1', 'Arranged Overdraft', 'Unarranged Overdraft') then 1
                                                 else maturity/12 end)) as InitialPD_lifetime

, case when CurrentRating in ('18', '19') then 1 
       else  1.0 - pow(1.0 - PD , maturity/12) end as PD_lifetime

-- down

, case when CurrentRating in ('18', '19') then 1 
       else  1.0 - pow(1.0 - PD_down , maturity/12) end as PD_lifetime_down

-- neu

, case when CurrentRating in ('18', '19') then 1 
       else  1.0 - pow(1.0 - PD_neu , maturity/12) end as PD_lifetime_neu

-- up 

, case when CurrentRating in ('18', '19') then 1 
       else  1.0 - pow(1.0 - PD_up , maturity/12) end as PD_lifetime_up

-- Scenarios weighted PD lifetime

, 2.5*PD_lifetime_down/100 + 95*PD_lifetime_neu/100 + 2.5*PD_lifetime_up/100 as PD_lifetime_scenarios

, case when fb.instrument_id is not null then true else false end as forbearance_flag
, case when fb.instrument_id is not null then fb.forbearance_reason else 'no_forbearance' end as forbearance_reason

, case when d.user_id is not null or ins.user_id is not null or sch.user_id is not null then 3
       when (df.user_id is not null) 
            and (c.reportdate::date between df.default_during_forbearance_date::date and date_add('month', 12, df.default_during_forbearance_date::date)) then 3
       when c.dpd > 90 then 3
       when c.dpd > 30 then 2
       when CurrentRating in ('15','16','17') then 2 

       -- uneployment benefits (ever, on the reporting date or what?)
       when sb.user_id is not null then 2

       -- employers bankruptcy (where to find?)

       -- active garnishment 

       when g.user_id is not null then 2

       -- active P-accounts (in progress)
       when pa.user_id is not null then 2

       -- Credit Bureau

       when schr.provider = 'SCHUFA' and schr.rating in ('N','O' ,'P') then 2 

       when schr.provider = 'CRIF' and schr.rating in ('M' ,'P') then 2

       -- HRC

       when hrc.user_id is not null then  2

       -- significant increase in PD

       when InitialPD_lifetime <> 0 and PD_lifetime / InitialPD_lifetime > 3 then 2 -- add loans!

       when fly.instrument_id is not null then 2 

       -- Other portfolio - Stage_1 

       else 1
       end Stage

, case when d.user_id is not null or ins.user_id is not null or sch.user_id is not null then 3
       when (df.user_id is not null) 
            and (c.reportdate::date between df.default_during_forbearance_date::date and date_add('month', 12, df.default_during_forbearance_date::date)) then 3
       when c.dpd > 90 then 3
       when c.dpd > 30 then 2
       when CurrentRating in ('15','16','17') then 2 

       -- uneployment benefits (ever, on the reporting date or what?)
       when sb.user_id is not null then 2

       -- employers bankruptcy (where to find?)

       -- active garnishment 

       when g.user_id is not null then 2

       -- active P-accounts (in progress)
       when pa.user_id is not null then 2

       -- Credit Bureau

       when schr.provider = 'SCHUFA' and schr.rating in ('N','O' ,'P') then 2 

       when schr.provider = 'CRIF' and schr.rating in ('M' ,'P') then 2

       -- HRC

       when hrc.user_id is not null then  2

       -- significant increase in PD

       when InitialPD_lifetime <> 0 and PD_lifetime_scenarios / InitialPD_lifetime > 3 then 2 -- add loans!

       when fly.instrument_id is not null then 2 

       -- Other portfolio - Stage_1 

       else 1
       end Stage_scenarios 


, case when d.user_id is not null or ins.user_id is not null or sch.user_id is not null then 'default_via_dunning_insolvency'
       when (df.user_id is not null) 
            and (c.reportdate::date between df.default_during_forbearance_date::date and date_add('month', 12, df.default_during_forbearance_date::date)) then 'default_via_fbe'
       when c.dpd > 90 then 'default_via_dpd'
       
       when c.dpd > 30 then 'dpd_30+'

       when CurrentRating in ('15','16','17') then 'rating_15_16_17'
       

        -- uneployment benefits (ever, on the reporting date or what?)
       when sb.user_id is not null then 'unemployment_or_other_benefits'
       -- employers bankruptcy ?

       -- active garnishment

       when g.user_id is not null then 'Garmishment_cancellation'
       -- active P-accounts

       when pa.user_id is not null then 'P_account'

       -- Credit Bureau

       when schr.provider = 'SCHUFA' and schr.rating in ('N','O' ,'P') then 'SCHUFA_NOP'

       when schr.provider = 'CRIF' and schr.rating in ('M' ,'P') then 'CRIF_MP'

       -- HRC

       when hrc.user_id is not null then  'HRC'

       -- significant increase in PD
       when InitialPD_lifetime <> 0  and PD_lifetime_scenarios / InitialPD_lifetime > 3 then '200+%_pd_increase' -- double check 

       when fly.instrument_id is not null then 'Forbearance'

       else 'Stage_1'
       end Stage_scenarios_description 


-- 16. SLLP 

, LGD * case when CurrentRating in ('18', '19') then OnBalance_EAD else 0 end SLLP

-- per product

, LGD * case when CurrentRating in ('18', '19') then c.OD_exposures else 0 end SLLP_OD
, LGD * case when CurrentRating in ('18', '19') then CC_exposures else 0 end SLLP_CC
, LGD * case when CurrentRating in ('18', '19') then TBIL_exposures else 0 end SLLP_TBIL
, LGD * case when CurrentRating in ('18', '19') then RP_2_exposures else 0 end SLLP_RP_2


-- 17. GLLP

, LGD * PD * case when CurrentRating not in ('18', '19') and c.product_flag in ('RP_1'
                                                                                , 'Arranged Overdraft'
                                                                                , 'Unarranged Overdraft') then  (OnBalance_EAD + OffBalance_EAD)
                        else 0 end GLLP_OD

, LGD * (1.0 - pow((1.0 - PD) , case when  maturity > 12 and stage  = 2 then maturity/12
                                 when  maturity <= 12  then maturity/12
                                 else 1 end)) * case when CurrentRating not in ('18', '19') then RP_2_exposures 
                                                     else 0 end as GLLP_RP_2

, LGD * (1.0 - pow((1.0 - PD) , case when maturity > 12 and stage  = 2 then maturity/12
                                 when maturity <= 12  then  maturity/12
                                 else 1 end)) * case when CurrentRating not in ('18', '19') then CC_exposures 
                                                     else 0 end as GLLP_CC

, LGD * (1.0 - pow((1.0 - PD) , case when maturity > 12 and stage  = 2 then maturity/12 
                                 when maturity <= 12  then  maturity/12
                                 else 1 end)) * case when CurrentRating not in ('18', '19') then TBIL_exposures
                                            else 0 end as GLLP_TBIL

-- GLLP new 

, LGD * (1.0 - pow((1.0 - PD) , case when c.product_flag in ('RP_1', 'Arranged Overdraft', 'Unarranged Overdraft') then 1
                                 when maturity > 12 and stage  = 2  then maturity/12 
                                 when maturity <= 12 then maturity/12 
                                 else 1 end)) * case when CurrentRating not in ('18', '19') then (OnBalance_EAD + OffBalance_EAD)
                                                     else 0 end as GLLP

-- by scenarios

, LGD * PD_scen_weighted * case when CurrentRating_scen_weighted not in ('18', '19') and c.product_flag in ('RP_1'
                                                                                , 'Arranged Overdraft'
                                                                                , 'Unarranged Overdraft') then  (OnBalance_EAD + OffBalance_EAD)
                        else 0 end GLLP_OD_scen_weighted

, LGD * (1.0 - pow((1.0 - PD_scen_weighted) , case when  maturity > 12 and Stage_scenarios  = 2 then maturity/12
                                 when  maturity <= 12  then maturity/12
                                 else 1 end)) * case when CurrentRating_scen_weighted not in ('18', '19') then RP_2_exposures 
                                                     else 0 end as GLLP_RP_2_scen_weighted

, LGD * (1.0 - pow((1.0 - PD_scen_weighted) , case when maturity > 12 and Stage_scenarios  = 2 then maturity/12
                                 when maturity <= 12  then  maturity/12
                                 else 1 end)) * case when CurrentRating_scen_weighted not in ('18', '19') then CC_exposures 
                                                     else 0 end as GLLP_CC_scen_weighted

, LGD * (1.0 - pow((1.0 - PD_scen_weighted) , case when maturity > 12 and Stage_scenarios  = 2 then maturity/12 
                                 when maturity <= 12  then  maturity/12
                                 else 1 end)) * case when CurrentRating_scen_weighted not in ('18', '19') then TBIL_exposures
                                            else 0 end as GLLP_TBIL_scen_weighted

-- down

, LGD * (1.0 - pow((1.0 - PD_down) , case when c.product_flag in ('RP_1', 'Arranged Overdraft', 'Unarranged Overdraft') then 1
                                 when maturity > 12 and Stage_scenarios  = 2  then maturity/12 
                                 when maturity <= 12 then maturity/12 
                                 else 1 end)) * case when CurrentRating_down not in ('18', '19') then (OnBalance_EAD + OffBalance_EAD)
                                                     else 0 end as GLLP_down
-- neu
, LGD * (1.0 - pow((1.0 - PD_neu) , case when c.product_flag in ('RP_1', 'Arranged Overdraft', 'Unarranged Overdraft') then 1
                                 when maturity > 12 and Stage_scenarios  = 2  then maturity/12 
                                 when maturity <= 12 then maturity/12 
                                 else 1 end)) * case when CurrentRating_neu not in ('18', '19') then (OnBalance_EAD + OffBalance_EAD)
                                                     else 0 end as GLLP_neu

-- up 

, LGD * (1.0 - pow((1.0 - PD_up) , case when c.product_flag in ('RP_1', 'Arranged Overdraft', 'Unarranged Overdraft') then 1
                                 when maturity > 12 and Stage_scenarios  = 2  then maturity/12 
                                 when maturity <= 12 then maturity/12 
                                 else 1 end)) * case when CurrentRating_up not in ('18', '19') then (OnBalance_EAD + OffBalance_EAD)
                                                     else 0 end as GLLP_up

, LGD * (1.0 - pow((1.0 - PD_scen_weighted::float) , case when c.product_flag in ('RP_1', 'Arranged Overdraft', 'Unarranged Overdraft') then 1
                                 when maturity > 12 and Stage_scenarios  = 2  then maturity/12 
                                 when maturity <= 12 then maturity/12 
                                 else 1 end)) * case when CurrentRating_scen_weighted not in ('18', '19') then (OnBalance_EAD + OffBalance_EAD)
                                                     else 0 end as GLLP_scen_weighted

-- ECL split 

, (LGD * (1.0 - pow((1.0 - PD_scen_weighted) , case when c.product_flag in ('RP_1', 'Arranged Overdraft', 'Unarranged Overdraft') then 1
                                 when maturity > 12 and Stage_scenarios  = 2  then maturity/12 
                                 when maturity <= 12 then maturity/12 
                                 else 1 end)) * case when CurrentRating_scen_weighted not in ('18', '19') then (OnBalance_EAD)
                                                     else 0 end) +  (LGD * case when CurrentRating in ('18', '19') then OnBalance_EAD else 0 end) as ECL_On_scen_weighted

, LGD * (1.0 - pow((1.0 - PD_scen_weighted) , case when c.product_flag in ('RP_1', 'Arranged Overdraft', 'Unarranged Overdraft') then 1
                                 when maturity > 12 and Stage_scenarios  = 2  then maturity/12 
                                 when maturity <= 12 then maturity/12 
                                 else 1 end)) * case when CurrentRating_scen_weighted not in ('18', '19') then (OffBalance_EAD)
                                                     else 0 end as ECL_Off_scen_weighted



-- Fields for Reg. Reporting 

, case when c.is_default = 1 then 'J' else 'N' end as "AUSFL"

, CASE
    WHEN c.is_default = 1 THEN
        GREATEST(
            COALESCE(c.credit_creation_date, DATE '0001-01-01'),
            COALESCE(
                CASE 
                    WHEN d.user_id IS NOT NULL THEN d.default_date
                    WHEN ins.user_id IS NOT NULL THEN ins.internal_insolvency_date
                    WHEN sch.user_id IS NOT NULL THEN c.schufa_rating_date
                    WHEN c.dpd > 90 THEN date_add('day', 90, c.start_in_arrears)
                END,
                DATE '0001-01-01'
            )
        )
    ELSE NULL
END AS "DXAUD"

, case when sch.user_id is not null then 'J'  -- debtor applies for insolvency
       when ins.user_id is not null then 'I'  -- institution aplies for debtor insolvency
       when d.user_id is not null then 'K' -- TBD with Anja, here - OD cancellation & forbearance (should be here?)
       when (df.user_id is not null)
            and (c.reportdate::date between df.default_during_forbearance_date::date and date_add('month', 12, df.default_during_forbearance_date::date)) then 'K'
       when c.dpd > 90 then 'C'
       when c.PD > 0.04 then 'B'
       else 'A' end "RISGR"
       

-- RISGR

, CASE
    WHEN c.is_default = 1 THEN
        GREATEST(
            COALESCE(c.credit_creation_date, DATE '0001-01-01'),
            COALESCE(
                CASE 
                    WHEN d.user_id IS NOT NULL THEN d.default_date
                    WHEN ins.user_id IS NOT NULL THEN ins.internal_insolvency_date
                    WHEN sch.user_id IS NOT NULL THEN c.schufa_rating_date
                    WHEN df.user_id IS NOT NULL
                         AND c.reportdate::date BETWEEN df.default_during_forbearance_date::date 
                                                   AND date_add('month', 12, df.default_during_forbearance_date::date)
                    THEN df.default_during_forbearance_date::date
                    WHEN c.dpd > 90 THEN date_add('day', 90, c.start_in_arrears)
                END,
                DATE '0001-01-01'
            )
        )
    ELSE NULL
END AS "DXNPE"

, case when is_default = 1 and max(case when c.product_flag in ('RP_2') then 1 else 0 end) over(partition by c.user_id) = 1 
            then max(fb.credit_creation_date) over(partition by c.user_id) end as "DXFBE"

, case when fb.instrument_id is not null then 3290 end as "FBSFI"

, case when Stage_scenarios = 1 then 5020 
       when Stage_scenarios = 2 then 5021 
       when Stage_scenarios = 3 then 5022 end "IMSFI"

, case when is_default = 1 then 3292
       when is_default = 0 
            and ld.last_default_date::date between date_add('month', -12, c.reportdate::date) and c.reportdate::date
            then 3294
       else 3293 end "PFSFI"

, case when is_default = 1 and coalesce(c.dpd,0) <= 90 then 3296
       when coalesce(c.dpd,0) <= 30 then 3295
       when coalesce(c.dpd,0) <= 90 then 5032 
       when coalesce(c.dpd,0) <= 180 then 2792
       when coalesce(c.dpd,0) <= 360 then 2785
       when coalesce(c.dpd,0) <= 720 then 3280
       when coalesce(c.dpd,0) <= 1800 then 3282
       when coalesce(c.dpd,0) <= 2520 then 6934
       when coalesce(c.dpd,0) > 2520 then 6935
       end as "TPDFI"

from "n26"."credit_risk_playground"."bp_retail_provisions_stg_v5_pre_updated_v2" c 
left join "n26"."credit_risk_playground"."bp_portfolio_balance_aud_m_v2" o on o.encoded_key = c.instrument_id and c.reportdate::date = o.reporting_date::date
left join "n26"."credit_risk_playground"."bp_overdraft_split_m_v2"  mll on mll.encoded_key = c.instrument_id and c.reportdate::date = mll.reporting_date::date
left join "n26"."credit_risk_playground"."bp_monitoring_pd_supermodel_v2_m_v2" m on m.user_id = c.user_id and m.reporting_date::date = c.reportdate::date
left join "n26"."credit_risk_playground"."bp_application_pd_supermodel_m_v2" pd_app on pd_app.encoded_key = c.instrument_id 
                                                                and  pd_app.reporting_date::date = c.reportdate::date
                                                                and c.product_flag not in ('Unarranged Overdraft', 'RP_2')
left join "n26"."credit_risk_playground"."bp_application_pd_supermodel_m_ad_v2" pd_app_ad on pd_app_ad.encoded_key = c.instrument_id 
                                                                and  pd_app_ad.reporting_date::date = c.reportdate::date
                                                                and c.product_flag not in ('Unarranged Overdraft', 'RP_2')
left join "n26"."credit_risk_playground"."bp_default_dunning_m_v2" d on d.user_id = c.user_id  and d.reporting_date::date = c.reportdate::date   
left join "n26"."credit_risk_playground"."bp_default_internal_insolvency_v2_m_v2" ins on ins.user_id = c.user_id and ins.reporting_date::date = c.reportdate::date
left join "n26"."credit_risk_playground"."bp_default_credit_bureau_insolvency_v3_m_v2" sch on sch.user_id = c.user_id and sch.reporting_date::date = c.reportdate::date
left join "n26"."credit_risk_playground"."bp_repayment_plan_ph1_m_v2" rp on rp.user_id = c.user_id and rp.reporting_date::date = c.reportdate::date
left join "n26"."credit_risk_playground"."bp_credit_bureau_rating_m_v2" schr on schr.user_id = c.user_id and schr.reporting_date::date = c.reportdate::date
left join first_enabled_date f on f.encoded_key = c.instrument_id and f.user_id = c.user_id and f.first_creation_date::date <= last_day(date_add('month', -1, last_day(getdate())))
left join credit_risk_playground.porto_pit_app_users pd_sc on pd_sc.instrument_id = c.instrument_id 
                                                              and c.product_flag in ('CC', 'TBIL')
                                                              and pd_sc.product_flag = c.product_flag
left join credit_risk_playground.porto_pit_app_users pd_od on pd_od.user_id = c.user_id 
                                                               and pd_od.product_flag in ('Arranged Overdraft', 'RP_1')
                                                               and c.product_flag in ('Arranged Overdraft', 'RP_1')
                                                              -- and pd_od.product_flag = c.product_flag
                                                               and pd_od.credit_creation_date::date = coalesce(f.first_creation_date::date,last_day(date_add('month', -1, last_day(getdate()))))
                                                               
left join repayment r on r.parent_account_key = c.instrument_id
left join  "n26"."dbt"."zrh_users" z on z.user_id = c.user_id 
left join "n26"."credit_risk_playground"."bp_loan_contract_information_m_v2" li on li.encoded_key = c.instrument_id and li.reporting_date::date = c.reportdate::date
left join "n26"."public"."mmbr_savings_account" s on s.encoded_key = c.instrument_id and s.account_type = 'CURRENT_ACCOUNT'
left join "n26"."credit_risk_playground"."bp_garnishment_m_v2" g on g.user_id = c.user_id 
                                             and c.reportdate::date = g.reporting_date::date  
left join "n26"."credit_risk_playground"."bp_social_benefits_v2_m_v2" sb on sb.user_id = c.user_id and sb.reporting_date::date = c.reportdate::date
left join "n26"."credit_risk_playground"."bp_p_accounts_v2" pa on pa.user_id = c.user_id and last_day(date_add('month', -1, last_day(getdate()))) between pa.rev_timestamp and pa.end_timestamp
left join "n26"."credit_risk_playground"."bp_hrc_m_v2" hrc on c.user_id = hrc.user_id and c.reportdate::date = hrc.reporting_date::date
left join forbearance_last_year fly on fly.instrument_id = c.instrument_id 
left join "n26"."credit_risk_playground"."bp_forbearance_m_v2_v2" fb on fb.instrument_id = c.instrument_id and c.reportdate::date = fb.reportdate::date
left join latest_default_during_forbearance df on df.user_id = c.user_id
left join default_last_12_months ld on ld.user_id = c.user_id
where c.reportdate::date = last_day(date_add('month', -1, last_day(getdate())))
order by 1
)

select * 
, getdate() as etl_updated
, coalesce(instrument_id, '') || coalesce(etl_updated::varchar, '') as unique_key
from total 

where etl_updated > (select max(etl_updated) from "n26"."credit_risk_playground"."bp_retail_provisions_stg_v5_v4_updated_v2")
