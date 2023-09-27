CREATE OR REPLACE TABLE keepcoding.ivr_summary AS
WITH calls 
AS (SELECT 
      ivr_id
      , LAG(start_date) OVER (PARTITION BY phone_number ORDER BY start_date) AS llamada_previa
      , LEAD(start_date) OVER (PARTITION BY phone_number ORDER BY start_date) AS siguiente_llamada
    FROM keepcoding.ivr_calls)
SELECT 
    detail.calls_ivr_id
  , calls_phone_number
  , calls_ivr_result
  , CASE WHEN STARTS_WITH(calls_vdn_label, "ATC") THEN "FRONT"
         WHEN STARTS_WITH(calls_vdn_label, "TECH") THEN "TECH"
         WHEN calls_vdn_label = "ABSORPTION" THEN "ABSORPTION"
    ELSE "RESTO"
    END AS calls_vdn_aggregation
  , calls_start_date
  , calls_end_date
  , calls_total_duration
  , calls_customer_segment
  , calls_ivr_language
  , calls_steps_module
  , calls_module_aggregation
  , COALESCE(detail.document_type, steps.document_type) as document_type
  , COALESCE(detail.document_identification, steps.document_identification) as document_identification
  , COALESCE(detail.customer_phone, steps.customer_phone) as customer_phone
  , COALESCE(detail.billing_account_id, steps.billing_account_id) as billing_account_id
  , IF(CONTAINS_SUBSTR(calls_module_aggregation, "AVERIA_MASIVA"), 1, 0) AS masiva_lg
  , IF(detail.step_name = "CUSTOMERINFOBYPHONE.TX" AND detail.step_description_error IS NULL, 1,0) as info_by_phone_lg
  , IF(detail.step_name = "CUSTOMERINFOBYDNIE.TX" AND detail.step_description_error IS NULL,1, 0) as info_by_dni_lg
  , IF(DATETIME_DIFF(detail.calls_start_date, calls.llamada_previa,HOUR)<24,1,0) AS repeated_phone_24H
  , IF(DATETIME_DIFF(calls.siguiente_llamada,detail.calls_end_date,HOUR)<24,1,0) AS cause_recall_phone_24H
FROM keepcoding.ivr_detail detail
LEFT 
  JOIN keepcoding.ivr_steps steps
  ON steps.ivr_id = detail.calls_ivr_id
LEFT
  JOIN calls
  ON detail.calls_ivr_id = calls.ivr_id
QUALIFY ROW_NUMBER() OVER(PARTITION BY CAST(detail.calls_ivr_id AS STRING) ORDER BY detail.calls_ivr_id,detail.calls_start_date DESC) = 1


