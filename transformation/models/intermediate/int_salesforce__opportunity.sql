WITH stage AS (

  SELECT * FROM {{ ref('stg_salesforce__opportunity') }}

),

reformatted AS (

  SELECT

--keys
    opportunity_id,
    accountid               AS account_id,
    contactid               AS contact_id,
    contractid              AS contract_id,
    pricebook2id            AS pricebook_id,
    campaignid              AS campaign_id,
    ownerid                 AS opportunity_owner__user_id,
    createdbyid             AS opportunity_created_by__user_id,
    lastmodifiedbyid        AS opportunity_last_modified_by__user_id,
    primarypartneraccountid AS opportunity_primary_partner__account_id,

-- renamed fields
    name                          AS opportunity_name,
    description                   AS opportunity_description,
    stagename                     AS opportunity_stage_name,
    stagesortorder                AS opportunity_stage_sort_order,
    amount                        AS opportunity_amount,
    probability                   AS opportunity_probability,
    expectedrevenue               AS opportunity_expected_revenue,
    totalopportunityquantity      AS total_opportunity_quantity,
    type                          AS opportunity_type,
    nextstep                      AS next_step,
    leadsource                    AS opportunity_lead_source,
    forecastcategory              AS opportunity_forecast_category,
    forecastcategoryname          AS forecast_category_name,
    fiscalyear                    AS fiscal_year,
    fiscalquarter                 AS fiscal_quarter,
    deliveryinstallationstatus__c AS opportunity_delivery_installation_status,
    trackingnumber__c             AS opportunity_tracking_number,
    ordernumber__c                AS opportunity_order_number,
    currentgenerators__c          AS opportunity_current_generators,
    maincompetitors__c            AS opportunity_main_competitors,

-- boolean conversions
    {{ convert_to_boolean('isdeleted') }}              AS is_opportunity_deleted,
    {{ convert_to_boolean('isprivate') }}              AS is_opportunity_private,
    {{ convert_to_boolean('isclosed') }}               AS is_opportunity_closed,
    {{ convert_to_boolean('iswon') }}                  AS is_opportunity_won,
    {{ convert_to_boolean('hasopportunitylineitem') }} AS has_opportunity_line_item,

-- date conversions
    {{ dbt_date.convert_timezone('closedate', source_tz='America/Los_Angeles') }}           AS date_opportunity_close,
    {{ dbt_date.convert_timezone('createddate', source_tz='America/Los_Angeles') }}         AS date_opportunity_created,
    {{ dbt_date.convert_timezone('lastmodifieddate', source_tz='America/Los_Angeles') }}    AS date_opportunity_last_modified,
    {{ dbt_date.convert_timezone('lastactivitydate', source_tz='America/Los_Angeles') }}    AS date_last_activity,
    {{ dbt_date.convert_timezone('laststagechangedate', source_tz='America/Los_Angeles') }} AS date_last_stage_change,
    {{ dbt_date.convert_timezone('systemmodstamp', source_tz='America/Los_Angeles') }}      AS date_system_mod_stamp

  FROM stage

)

SELECT * FROM reformatted
