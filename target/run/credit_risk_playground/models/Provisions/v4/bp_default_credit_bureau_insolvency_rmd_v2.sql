
  
    

  create  table
    "n26"."credit_risk_playground"."bp_default_credit_bureau_insolvency_rmd_v2__dbt_tmp"
    
    
    
  as (
    


with 

raw_merkmal_description as (SELECT  ca.user_id,
            ca.provider,
            ca.audit_rev_timestamp AS rev_timestamp,
            ca.etl_updated,
            COALESCE(
                    NULLIF(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(
                                                          JSON_EXTRACT_ARRAY_ELEMENT_TEXT(
                                                                  JSON_EXTRACT_PATH_TEXT(ca.response_raw_value, 'data', 'reaktion', TRUE),
                                                                  0), 'bonitaetsauskunft',
                                                          'merkmale', TRUE),
                                                  'merkmalOrTextmerkmal'), ''),
                    NULLIF(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(
                                                          JSON_EXTRACT_ARRAY_ELEMENT_TEXT(
                                                                  JSON_EXTRACT_PATH_TEXT(ca.response_raw_value, 'data', 'reaktion', TRUE),
                                                                  0), 'nachmeldung',
                                                          'merkmale', TRUE),
                                                  'merkmalOrTextmerkmal'), '')
                )               AS merkmal,
            CASE
                WHEN is_valid_json(ca.response_raw_value) THEN json_parse(ca.response_raw_value)
                ELSE json_parse('{}')
                END
            AS response_value
     FROM private.californium_credit_score_record_request_audit_log ca
     inner join credit_risk_playground.bp_default_credit_bureau_insolvency_max_date_v2 m
        on m.user_id = ca.user_id
        and ca.audit_rev_timestamp::timestamp = m.rev_timestamp::timestamp
     WHERE provider in ('SCHUFA', 'CRIF')
     )

     select * 
     from raw_merkmal_description
  );
  