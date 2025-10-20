
      
        
            
        delete from "n26"."credit_risk_playground"."bp_monitoring_pd_supermodel_v2_m_ik"
    where (unique_key) in (
        select distinct unique_key
        from "bp_monitoring_pd_supermodel_v2_m_ik__dbt_tmp145655287794" as DBT_INTERNAL_SOURCE
    )
    
    ;
    

    insert into "n26"."credit_risk_playground"."bp_monitoring_pd_supermodel_v2_m_ik" ("user_id", "calculated_at", "rating_class", "pd_2", "calibration_sgm", "internal_pd", "reporting_date", "etl_updated", "unique_key")
        (
            select "user_id", "calculated_at", "rating_class", "pd_2", "calibration_sgm", "internal_pd", "reporting_date", "etl_updated", "unique_key"
            from "bp_monitoring_pd_supermodel_v2_m_ik__dbt_tmp145655287794"
        )
  