
      
        
            
        delete from "n26"."credit_risk_playground"."bp_overdraft_split_m_v2"
    where (unique_key) in (
        select distinct unique_key
        from "bp_overdraft_split_m_v2__dbt_tmp120413304325" as DBT_INTERNAL_SOURCE
    )
    
    ;
    

    insert into "n26"."credit_risk_playground"."bp_overdraft_split_m_v2" ("user_id", "encoded_key", "balance", "max_date_neg_balance", "reporting_date", "od_type", "overdraft_lim", "etl_updated", "unique_key")
        (
            select "user_id", "encoded_key", "balance", "max_date_neg_balance", "reporting_date", "od_type", "overdraft_lim", "etl_updated", "unique_key"
            from "bp_overdraft_split_m_v2__dbt_tmp120413304325"
        )
  