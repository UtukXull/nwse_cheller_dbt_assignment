SELECT
  opportunity_id,
  opportunity_created__date_id,
  opportunity_close__date_id

FROM {{ ref('mrt_salesforce__fact_opportunity_history') }}
WHERE
  opportunity_close__date_id < opportunity_created__date_id
  AND opportunity_close__date_id IS NOT NULL
