SELECT
  opportunity_id,
  opportunity_probability,
  opportunity_amount,
  opportunity_expected_revenue

FROM {{ ref('int_salesforce__opportunity_history') }}
WHERE
  opportunity_expected_revenue != ((opportunity_probability / 100) * opportunity_amount)
