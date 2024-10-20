SELECT
  opportunity_id,
  opportunity_amount,
  opportunity_expected_revenue

FROM {{ ref('mrt_salesforce__fact_opportunity') }}
WHERE
  opportunity_amount < 0
  OR opportunity_expected_revenue < 0
