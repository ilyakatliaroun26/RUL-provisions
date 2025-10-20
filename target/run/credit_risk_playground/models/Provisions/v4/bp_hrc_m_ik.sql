
      
        
            
        delete from "n26"."credit_risk_playground"."bp_hrc_m_ik"
    where (unique_key) in (
        select distinct unique_key
        from "bp_hrc_m_ik__dbt_tmp152356053278" as DBT_INTERNAL_SOURCE
    )
    
    ;
    

    insert into "n26"."credit_risk_playground"."bp_hrc_m_ik" ("user_id", "dash_id", "nickel_id", "dash_created_ts", "dash_recorded_at", "type", "dash_subject", "nickel_event_start", "nickel_event_end", "nickel_operation", "reporting_date", "etl_updated", "unique_key")
        (
            select "user_id", "dash_id", "nickel_id", "dash_created_ts", "dash_recorded_at", "type", "dash_subject", "nickel_event_start", "nickel_event_end", "nickel_operation", "reporting_date", "etl_updated", "unique_key"
            from "bp_hrc_m_ik__dbt_tmp152356053278"
        )
  