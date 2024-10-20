SELECT
  opportunity_id,
  date_opportunity_record_valid_from,
  date_opportunity_record_valid_through

FROM {{ ref('mrt_salesforce__fact_opportunity_history') }}
WHERE
  COALESCE(date_opportunity_record_valid_through, (CURRENT_DATE() + INTERVAL '100 years') :: TIMESTAMP) < date_opportunity_record_valid_from
