
      
        
            
        delete from "n26"."credit_risk_playground"."bp_portfolio_balance_aud_m_v2"
    where (unique_key) in (
        select distinct unique_key
        from "bp_portfolio_balance_aud_m_v2__dbt_tmp172855938022" as DBT_INTERNAL_SOURCE
    )
    
    ;
    

    insert into "n26"."credit_risk_playground"."bp_portfolio_balance_aud_m_v2" ("user_id", "encoded_key", "reporting_date", "product", "balance", "unique_key", "etl_updated")
        (
            select "user_id", "encoded_key", "reporting_date", "product", "balance", "unique_key", "etl_updated"
            from "bp_portfolio_balance_aud_m_v2__dbt_tmp172855938022"
        )
  