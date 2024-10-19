

with stage AS (

  SELECT * FROM {{ ref('stg_salesforce__campaign') }}

),

reformatted AS (

  SELECT

--keys
    campaign_id,
    parentid                   AS parent__campaign_id,
    ownerid                    AS campaign_owner__user_id,
    createdbyid                AS created_by__user_id,
    lastmodifiedbyid           AS last_modified_by__user_id,
    campaignmemberrecordtypeid AS campaign_member_record_type_id,

-- renamed fields
    name                              AS campaign_name,
    type                              AS campaign_type,
    status                            AS campaign_status,
    expectedrevenue                   AS expected_revenue,
    budgetedcost                      AS budgeted_cost,
    actualcost                        AS actual_cost,
    expectedresponse                  AS expected_response,
    numbersent                        AS number_sent,
    description                       AS campaign_description,
    numberofleads                     AS number_of_leads,
    numberofconvertedleads            AS number_of_converted_leads,
    numberofcontacts                  AS number_of_contacts,
    numberofresponses                 AS number_of_responses,
    numberofopportunities             AS number_of_opportunities,
    numberofwonopportunities          AS number_of_won_opportunities,
    amountallopportunities            AS amount_all_opportunities,
    amountwonopportunities            AS amount_won_opportunities,
    hierarchynumberofleads            AS hierarchy_number_of_leads,
    hierarchynumberofconvertedleads   AS hierarchy_number_of_converted_leads,
    hierarchynumberofcontacts         AS hierarchy_number_of_contacts,
    hierarchynumberofresponses        AS hierarchy_number_of_responses,
    hierarchynumberofopportunities    AS hierarchy_number_of_opportunities,
    hierarchynumberofwonopportunities AS hierarchy_number_of_won_opportunities,
    hierarchyamountallopportunities   AS hierarchy_amount_all_opportunities,
    hierarchyamountwonopportunities   AS hierarchy_amount_won_opportunities,
    hierarchynumbersent               AS hierarchy_number_sent,
    hierarchyexpectedrevenue          AS hierarchy_expected_revenue,
    hierarchybudgetedcost             AS hierarchy_budgeted_cost,
    hierarchyactualcost               AS hierarchy_actual_cost,
    lastactivitydate                  AS last_activity_date,

-- boolean conversions
    {{ convert_to_boolean('isdeleted') }} AS is_deleted,
    {{ convert_to_boolean('isactive') }}  AS is_active,

-- date conversions
    {{ dbt_date.convert_timezone('startdate', source_tz='America/Los_Angeles') }}        AS date_start,
    {{ dbt_date.convert_timezone('enddate', source_tz='America/Los_Angeles') }}          AS date_end,
    {{ dbt_date.convert_timezone('createddate', source_tz='America/Los_Angeles') }}      AS date_created,
    {{ dbt_date.convert_timezone('lastmodifieddate', source_tz='America/Los_Angeles') }} AS date_last_modified,
    {{ dbt_date.convert_timezone('systemmodstamp', source_tz='America/Los_Angeles') }}   AS date_system_mod_stamp

  FROM stage

)

SELECT * FROM reformatted
