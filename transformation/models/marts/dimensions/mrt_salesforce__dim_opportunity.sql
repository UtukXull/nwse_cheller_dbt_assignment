{{ config(alias = 'dim_salesforce_opportunity') }}

WITH base_opportunity AS (

  SELECT * FROM {{ ref('int_salesforce__opportunity') }}

),

opportunity AS (

  SELECT
    opportunity_id,
    opportunity_name,
    opportunity_stage_name,
    opportunity_stage_sort_order,
    opportunity_delivery_installation_status,
    opportunity_main_competitors,
    opportunity_order_number

  FROM base_opportunity

)

SELECT * FROM opportunity
