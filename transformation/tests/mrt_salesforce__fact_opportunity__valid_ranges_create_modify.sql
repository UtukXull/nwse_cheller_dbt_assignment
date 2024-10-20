SELECT
  opportunity_id,
  opportunity_created__date_id,
  opportunity_last_modified__date_id

FROM {{ ref('mrt_salesforce__fact_opportunity') }}
WHERE
  opportunity_created__date_id > opportunity_last_modified__date_id
