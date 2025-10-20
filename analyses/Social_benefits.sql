WITH recent_alg2_transactions AS 
( 
SELECT 
      t.account_id 
      ,t.created 
      ,t.currency 
      ,(t.bank_balance_impact * 100)::int AS bank_balance_impact_cents 
      ,t.partner_name
       ,t.reference_text 
FROM etl_reporting.zr_transaction t 
LEFT JOIN cr_account a ON t.account_id = a.id 
WHERE t.created >= DATE_TRUNC('month', '{reporting_date}'::timestamp AT TIME ZONE 'Europe/Berlin') 
      AND t.created < '{reporting_date}'::timestamp AT TIME ZONE 'Europe/Berlin' 
      AND bank_balance_impact > 0 
      AND t.partner_name LIKE '%Bundesagentur%Arbeit%' 
      AND t.partner_name NOT LIKE '%Familienkasse%' 
      AND ' / ' || SUBSTRING(reference_text, 1, 5) = SUBSTRING(reference_text, 15,8) /* ALG2 */ 
      AND a.account_role = 'PRIMARY' AND TRIM(a.status) <> 'CLOSED' )

, previous_bank_balances AS 
( 
SELECT 
      t.account_id 
      ,t.created 
      ,SUM(t2.bank_balance_impact) AS previous_bank_balance 
FROM recent_alg2_transactions t 
LEFT JOIN etl_reporting.zr_transaction t2 ON t.account_id = t2.account_id AND t2.created < t.created 
GROUP BY t.account_id ,t.created )

, recent_salary AS 
( 
SELECT 
      t.account_id 
      ,SUM(t.bank_balance_impact * 100)::int AS sum_salary_cents 
FROM etl_reporting.zr_transaction t 
LEFT JOIN cr_account a ON t.account_id = a.id 
WHERE t.created >= DATE_TRUNC('month',  '{reporting_date}'::timestamp AT TIME ZONE 'Europe/Berlin') 
      AND t.created < '{reporting_date}'::timestamp AT TIME ZONE 'Europe/Berlin' 
      AND bank_balance_impact > 0 
      AND (reference_text LIKE '%LOHN%' 
               OR reference_text LIKE '%Lohn%' 
               OR reference_text LIKE '%GEHALT%'
               OR reference_text LIKE '%Gehalt%') 
      AND a.account_role = 'PRIMARY' AND TRIM(a.status) <> 'CLOSED ' GROUP BY t.account_id )



, aggregation AS 
(
SELECT account_id 
       ,SUM(bank_balance_impact_cents) / 100 AS last_months_ALG2_amount 
       ,greatest(0, -MIN(previous_bank_balance)) AS last_months_max_utilisation_before_ALG2 
FROM previous_bank_balances 
INNER JOIN recent_alg2_transactions USING(account_id, created) 
GROUP BY account_id )

, alg2_OD_users AS 
( 
SELECT ua.user_created AT TIME ZONE 'UTC' AS user_created 
       ,ua.user_id 
       ,ua.account_id 
       ,o.amount_cents / 100 AS OD_limit 
       ,t.last_months_max_utilisation_before_ALG2 + COALESCE(sum_salary_cents, 0) / 100 AS potential_limit 
       ,t.last_months_ALG2_amount 
       ,COALESCE(sum_salary_cents, 0) / 100 AS last_month_salary 
FROM aggregation t 
INNER JOIN cr_user_account ua USING(account_id) 
LEFT JOIN recent_salary USING(account_id) 
INNER JOIN pu_overdraft o ON o.user_created = (ua.user_created AT TIME ZONE 'UTC') 
WHERE ua.user_id NOT IN ('b8d7ad25-1ca4-49c0-a188-0cfd280816a7') /* internal accounts */ 
      AND o.status = 'ENABLED' )

SELECT
      user_created 
      ,user_id::text 
      ,account_id::text 
      ,OD_limit 
      ,potential_limit 
      ,last_months_ALG2_amount 
      ,last_month_salary 
      ,CEILING(greatest(0, potential_limit / 10, -current_balance / 10)) * 10 AS proposed_limit 
FROM alg2_OD_users 
LEFT JOIN ( 
              SELECT 
                   account_id 
                   ,SUM(bank_balance_impact) AS current_balance 
              FROM etl_reporting.zr_transaction 
              WHERE account_id IN (
                                  SELECT account_id 
                                  FROM alg2_OD_users) 
              GROUP BY account_id 
              ) t USING(account_id) 
WHERE CEILING(greatest(0, potential_limit / 10, -current_balance / 10)) * 10 < OD_limit





