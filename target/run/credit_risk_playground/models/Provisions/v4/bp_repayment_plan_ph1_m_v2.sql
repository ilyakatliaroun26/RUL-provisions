
      
        
            
        delete from "n26"."credit_risk_playground"."bp_repayment_plan_ph1_m_v2"
    where (unique_key) in (
        select distinct unique_key
        from "bp_repayment_plan_ph1_m_v2__dbt_tmp122908505154" as DBT_INTERNAL_SOURCE
    )
    
    ;
    

    insert into "n26"."credit_risk_playground"."bp_repayment_plan_ph1_m_v2" ("user_id", "reporting_date", "etl_updated", "unique_key")
        (
            select "user_id", "reporting_date", "etl_updated", "unique_key"
            from "bp_repayment_plan_ph1_m_v2__dbt_tmp122908505154"
        )
  