
{{
  config(
    materialized = "incremental",
    dist='instrument_id',
    sort='etl_updated',
    unique_key="unique_key",
    tags=["retail_provisions"]
    )
}}

/*
Models to run

bp_portfolio_balance_aud_m_v2 +
bp_overdraft_limit_periods_v2   (first drop then create again) +
bp_portfolio_customers_aud_m_v2 + 
bp_overdue_amt_aud_m  (is not used anymore) +
bp_arrears_reg_aud  (use dbt.bp_arrears_reg_aud instead) 
bp_overdraft_split_m_v2 +
bp_monitoring_pd_supermodel_v2_m_v2 
bp_credit_bureau_rating_m_v2  
bp_default_dunning_m_v2 
bp_default_internal_insolvency_v2_m_v2
bp_default_credit_bureau_insolvency_max_date_v2 (first drop) +
bp_default_credit_bureau_insolvency_rmd_v2 (first drop) +
bp_default_credit_bureau_insolvency_me_v2 (first drop) +
bp_default_credit_bureau_insolvency_v3_m_v2 +
bp_repayment_plan_ph1_m_v2 +
bp_loan_contract_information_m_v2 +
bp_p_accounts_v2 (first drop) +
bp_hrc_m_v2 +
bp_social_benefits_v2_m_v2 +
bp_garnishment_m_v2
bp_application_pd_supermodel_m_v2 +
bp_application_pd_supermodel_m_ad_v2 +
bp_retail_provisions_stg_v5_pre_updated_v2 +
bp_forbearance_m_v2_v2
bp_retail_provisions_stg_v5_v4_updated_v2 +
bp_retail_provisions_m
*/

-- dbt run --full-refresh --select bp_retail_provisions_m_v3 (first time)
-- dbt run --select bp_retail_provisions_m_v3 (other runs)

with 

total as (
select distinct

-- 1. Customers base

user_id 
,  Instrument_ID
,  Country
,  LegalEntity
,  IS_IFRS -- "No" for TSY bonds and "Yes" for other instruments
,  ReportDate
,  CalcDate -- for retail same as eom, for mortgage 1-month lag
,  Script_calculation_date



-- 2. Product flag

, product_flag

, Instrument_Type

, Instrument 


-- 3. Creation date 

, credit_creation_date

-- 4. DPD

, start_in_arrears
, dpd
-- , coalesce(datediff('day', start_in_arrears::date, ReportDate::date) + 1,0) as dpd



-- 5. Default

, insolvency 
, dunning 
, schufa_insolvency
, rep_plan_user

, is_default


-- 6. Credit Bureau flags

, schufa_rating 
, provider
, schufa_rating_date

-- 7. Limit

, "Limit"

, last_limit_eur

, last_limit_date

-- 8. Exposures

, TBIL_exposures

, CC_exposures

, RP_2_exposures

, OD_exposures

-- , coalesce(lo.exposures_loan,0) as loan_exposures

-- 9. PD

-- Available Lisbon 2 PD


, logit

-- , (logit - 0.09089766913677097) / 0.9897750804490338 as logit_porto_corr

, pd_pit_calibrated_scen_weighted as pd_pit


, pd_pit_calibrated_scen_weighted as pd_od


-- Calibration PD

, pd_pit_calibrated_scen_weighted as pd_pit_calibrated

, rating_class_pit_calibrated_scen_weighted as rating_class_pit_calibrated

-- Final PD and rating (PIT)


, PD_scen_weighted as  PD

, CurrentRating_scen_weighted as CurrentRating

, CurrentRating_date



 -- 10. CCF

, utilization_pct
, usage


, ccf

, off_balance_exposure

-- 11. EAD

, OffBalance_EAD -- only for off-balance exposures by finance request

, OnBalance_EAD

, IsOffBalance



-- 12. LGD 

, case --when dxaud::date < date_add('year', -3, reportdate)::date then 1.0::float
     when product_flag in ('Arranged Overdraft', 'RP_1', 'RP_2') and sum(case when product_flag in ('Arranged Overdraft', 'RP_1', 'RP_2') then coalesce(OnBalance_EAD,0) else 0 end) over(partition by user_id) < 500.22 then 0.461767::float
     when product_flag in ('Arranged Overdraft', 'RP_1', 'RP_2') and sum(case when product_flag in ('Arranged Overdraft', 'RP_1', 'RP_2') then coalesce(OnBalance_EAD,0) else 0 end) over(partition by user_id) < 2751.38 then 0.521929::float
     when product_flag in ('Arranged Overdraft', 'RP_1', 'RP_2') and sum(case when product_flag in ('Arranged Overdraft', 'RP_1', 'RP_2') then coalesce(OnBalance_EAD,0) else 0 end) over(partition by user_id) < 5992.30 then 0.552278::float
     when product_flag in ('Arranged Overdraft', 'RP_1', 'RP_2') and sum(case when product_flag in ('Arranged Overdraft', 'RP_1', 'RP_2') then coalesce(OnBalance_EAD,0) else 0 end) over(partition by user_id) >= 5992.30 then 0.71094::float
     when product_flag in ('TBIL') and country = 'DEU' and sum(case when product_flag in ('TBIL') and country = 'DEU' then coalesce(OnBalance_EAD,0) else 0 end) over(partition by user_id) < 102.50 then 0.287018::float
     when product_flag in ('TBIL') and country = 'DEU' and sum(case when product_flag in ('TBIL') and country = 'DEU' then coalesce(OnBalance_EAD,0) else 0 end) over(partition by user_id) < 240.50 then 0.368662::float
     when product_flag in ('TBIL') and country = 'DEU' and sum(case when product_flag in ('TBIL') and country = 'DEU' then coalesce(OnBalance_EAD,0) else 0 end) over(partition by user_id) < 526.00 then 0.443474::float
     when product_flag in ('TBIL') and country = 'DEU' and sum(case when product_flag in ('TBIL') and country = 'DEU' then coalesce(OnBalance_EAD,0) else 0 end) over(partition by user_id) >= 526.00 then 0.520718::float
     when product_flag in ('TBIL') and country = 'FRA' and sum(case when product_flag in ('TBIL') and country = 'FRA' then coalesce(OnBalance_EAD,0) else 0 end) over(partition by user_id) < 43.50 then 0.840663::float
     when product_flag in ('TBIL') and country = 'FRA' and sum(case when product_flag in ('TBIL') and country = 'FRA' then coalesce(OnBalance_EAD,0) else 0 end) over(partition by user_id) < 83.50 then 0.846696::float
     when product_flag in ('TBIL') and country = 'FRA' and sum(case when product_flag in ('TBIL') and country = 'FRA' then coalesce(OnBalance_EAD,0) else 0 end) over(partition by user_id) < 123.50 then 0.878149::float
     when product_flag in ('TBIL') and country = 'FRA' and sum(case when product_flag in ('TBIL') and country = 'FRA' then coalesce(OnBalance_EAD,0) else 0 end) over(partition by user_id) >= 123.50 then 0.924742::float
     when product_flag in ('TBIL') and country in ('ESP', 'ITA') and sum(case when product_flag in ('TBIL') and country in ('ESP', 'ITA') then coalesce(OnBalance_EAD,0) else 0 end) over(partition by user_id) < 61.50 then 0.865395::float
     when product_flag in ('TBIL') and country in ('ESP', 'ITA') and sum(case when product_flag in ('TBIL') and country in ('ESP', 'ITA') then coalesce(OnBalance_EAD,0) else 0 end) over(partition by user_id) < 124.00 then 0.90235::float
     when product_flag in ('TBIL') and country in ('ESP', 'ITA') and sum(case when product_flag in ('TBIL') and country in ('ESP', 'ITA') then coalesce(OnBalance_EAD,0) else 0 end) over(partition by user_id) >= 124.00 then 0.941614::float
     when product_flag = 'CC' then 0.678207::float
     when product_flag = 'TBIL' then 0.287018::float
     when product_flag in ('Unarranged Overdraft') and country in ('AUT') then 0.81860::float
     when product_flag in ('Unarranged Overdraft') and country in ('BEL') then 0.84210::float
     when product_flag in ('Unarranged Overdraft') and country in ('DEU') then 0.77610::float
     when product_flag in ('Unarranged Overdraft') and country in ('ESP') then 0.93170::float
     when product_flag in ('Unarranged Overdraft') and country in ('FRA') then 0.87090::float
     when product_flag in ('Unarranged Overdraft') and country in ('IRL') then 0.70410::float
     when product_flag in ('Unarranged Overdraft') and country in ('ITA') then 0.84320::float
     when product_flag in ('Unarranged Overdraft') and country in ('NLD') then 0.77300::float
     when product_flag in ('Unarranged Overdraft') and country in ('POL') then 0.92630::float
     when product_flag in ('Unarranged Overdraft') and country in ('PRT') then 0.83770::float
     when product_flag in ('Unarranged Overdraft') then 0.9048::float
     else 0.80000::float end as LGD

-- 13. Finance

, overdraft_interest_accrued
-- , mi.interest_rate as interest_rate_overdraft
, interest_rate_loan
, accrued_interest_loan

, first_repayment
, last_repayment

, loan_amount 
, loan_name
, repayment_installments
, repayment_period_count
, principal_repayment_interval

-- 14. Maturity

, MaturityDate

, Maturity


-- 15. Stages 

-- for stage 2 reasons in https://docs.google.com/document/d/1xnfIHGd-4GTxUJ2QiZ04ImMk8yYe2EfBctrk3dW-Bjc/edit# : Section 5, only with Intensified Management
 --flag
 --and +200% PD increase as significat risk increase measure
 

-- application_date
, InitialPD_date

, InitialPD

, InitialPD_lifetime

, PD_lifetime_scenarios as PD_lifetime

, Stage_scenarios as Stage 


, Stage_scenarios_description as stage_description 


-- 16. SLLP 

, SLLP

-- per product

, SLLP_OD
, SLLP_CC
, SLLP_TBIL
, SLLP_RP_2


-- 17. GLLP

, GLLP_OD_scen_weighted as GLLP_OD

, GLLP_RP_2_scen_weighted as GLLP_RP_2

, GLLP_CC_scen_weighted as GLLP_CC

, GLLP_TBIL_scen_weighted as GLLP_TBIL

-- GLLP new 
, CASE 
    WHEN GLLP_scen_weighted < 0 THEN 0 
    ELSE GLLP_scen_weighted end as GLLP
-- ECL split 

, CASE 
    WHEN ECL_On_scen_weighted < 0 THEN 0 
    ELSE ECL_On_scen_weighted end as ECL_On

, ECL_Off_scen_weighted as ECL_Off


from {{ ref('bp_retail_provisions_stg_v5_v4_updated_v2') }} 
where ReportDate::date = last_day(
                            date_add('month'
                                    , -1
                                    , last_day(getdate())
                                    )
									)
    and not (
        (coalesce(onbalance_ead, 0) = 0 and coalesce("limit", 0) = 0)
        or
        (product_flag is null and coalesce(onbalance_ead, 0) + coalesce(offbalance_ead, 0) = 0)
    )
)


select * 
, getdate() as etl_updated
, coalesce(instrument_id, '') || coalesce(etl_updated::varchar, '') as unique_key
from total 
{% if is_incremental() %}
where etl_updated > (select max(etl_updated) from {{this}})
{% endif %}

