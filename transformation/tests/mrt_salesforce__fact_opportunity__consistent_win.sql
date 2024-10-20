SELECT
  opportunity_id,
  is_opportunity_won,
  is_opportunity_closed

FROM {{ ref('mrt_salesforce__fact_opportunity') }}
WHERE
  is_opportunity_won
  AND NOT is_opportunity_closed
