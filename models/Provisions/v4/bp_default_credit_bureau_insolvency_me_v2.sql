 
 
{{
    config(
    materialized = "table",
    tags=["retail_provisions"]
    )
}}

 
 with 
 
 merkmal_exploded AS (
    SELECT distinct
     raw_merkmal_description.user_id,
             raw_merkmal_description.provider,
             raw_merkmal_description.rev_timestamp,
             raw_merkmal_description.etl_updated,
             JSON_EXTRACT_ARRAY_ELEMENT_TEXT(
                     raw_merkmal_description.merkmal,
                     sequence.ordinal::INT,
                     TRUE
                 )                                                 AS merkmal,
             COALESCE(response_value.data.reaktion[0].bonitaetsauskunft,
                      response_value.data.reaktion[0].nachmeldung) AS reaction_type
      FROM credit_risk_playground.bp_default_credit_bureau_insolvency_rmd_v2 as raw_merkmal_description
      inner join dbt.zrh_users z on z.user_id = raw_merkmal_description.user_id
      CROSS JOIN public.generated_sequence AS sequence
           --only generate the number of records in the cross join that corresponds
           --to the number of merkmal in the order
      WHERE case when z.country_tnc = 'DEU' and 
	                  sequence.ordinal < JSON_ARRAY_LENGTH(raw_merkmal_description.merkmal, TRUE) then 1 
				 when z.country_tnc = 'AUT' then 1 
				 else 0 end = 1
                 )

select * 
from merkmal_exploded