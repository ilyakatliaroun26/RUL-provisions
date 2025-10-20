
      
        
            
        delete from "n26"."credit_risk_playground"."bp_forbearance_m_v2_ik"
    where (unique_key) in (
        select distinct unique_key
        from "bp_forbearance_m_v2_ik__dbt_tmp153758188935" as DBT_INTERNAL_SOURCE
    )
    
    ;
    

    insert into "n26"."credit_risk_playground"."bp_forbearance_m_v2_ik" ("user_id", "instrument_id", "reportdate", "credit_creation_date", "product_flag", "bal", "initialpd", "dpd_at_credit_decision", "cb_report_date", "cb_provider", "cb_rating", "forbearance_reason", "etl_updated", "unique_key")
        (
            select "user_id", "instrument_id", "reportdate", "credit_creation_date", "product_flag", "bal", "initialpd", "dpd_at_credit_decision", "cb_report_date", "cb_provider", "cb_rating", "forbearance_reason", "etl_updated", "unique_key"
            from "bp_forbearance_m_v2_ik__dbt_tmp153758188935"
        )
  