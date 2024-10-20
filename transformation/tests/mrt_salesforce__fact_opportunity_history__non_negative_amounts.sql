SELECT
  opportunity_id,
  date_opportunity_record_valid_from,
  opportunity_amount

FROM {{ ref('mrt_salesforce__fact_opportunity_history') }}
WHERE
  opportunity_amount < 0
  OR opportunity_previous_amount < 0
