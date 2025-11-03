
      
        
            
        delete from "n26"."credit_risk_playground"."bp_monitoring_pd_supermodel_v2_m_v2"
    where (unique_key) in (
        select distinct unique_key
        from "bp_monitoring_pd_supermodel_v2_m_v2__dbt_tmp154441258935" as DBT_INTERNAL_SOURCE
    )
    
    ;
    

    insert into "n26"."credit_risk_playground"."bp_monitoring_pd_supermodel_v2_m_v2" ("user_id", "calculated_at", "rating_class", "pd_2", "calibration_sgm", "internal_pd", "reporting_date", "etl_updated", "unique_key")
        (
            select "user_id", "calculated_at", "rating_class", "pd_2", "calibration_sgm", "internal_pd", "reporting_date", "etl_updated", "unique_key"
            from "bp_monitoring_pd_supermodel_v2_m_v2__dbt_tmp154441258935"
        )
  