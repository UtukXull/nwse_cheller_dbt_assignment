SELECT
  account_id,
  is_account_deleted,
  is_account_active

FROM {{ ref('int_salesforce__account') }}
WHERE
  is_account_deleted
  AND is_account_active
