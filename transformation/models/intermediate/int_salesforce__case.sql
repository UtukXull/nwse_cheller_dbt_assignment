

with stage AS (

  SELECT * FROM {{ ref('stg_salesforce__case') }}

),

reformatted AS (

  SELECT

--keys
    case_id,
    servicecontractid AS service_contract_id,
    parentid          AS parent__case_id,
    ownerid           AS case_owner__user_id,
    createdbyid       AS created_by__user_id,
    lastmodifiedbyid  AS last_modified_by__user_id,
    masterrecordid    AS master_record_id,
    casenumber        AS case_number,
    contactid         AS contact_id,
    accountid         AS account_id,
    assetid           AS asset_id,
    productid         AS product_id,
    entitlementid     AS entitlement_id,
    sourceid          AS source_id,
    businesshoursid   AS business_hours_id,

-- renamed fields
    suppliedname            AS supplied_name,
    suppliedemail           AS supplied_email,
    suppliedphone           AS supplied_phone,
    suppliedcompany         AS supplied_company,
    type                    AS case_type,
    status                  AS case_status,
    reason                  AS reason,
    origin                  AS case_origin,
    subject                 AS case_subject,
    priority                AS case_priority,
    description             AS case_description,
    engineeringreqnumber__c AS engineering_req_number,
    product__c              AS product,

-- boolean conversions
    {{ convert_to_boolean('isclosed') }}                           AS is_closed,
    {{ convert_to_boolean('isescalated') }}                        AS is_escalated,
    {{ convert_to_boolean('isclosedoncreate') }}                   AS is_closed_on_create,
    {{ convert_to_boolean('isstopped') }}                          AS is_stopped,
    {{ convert_to_boolean('isdeleted') }}                          AS is_deleted,
    {{ convert_to_boolean('slaviolation__c', 'yes', 'no') }}       AS is_sla_violation,
    {{ convert_to_boolean('potentialliability__c', 'yes', 'no') }} AS is_potential_liability,

-- date conversions
    {{ dbt_date.convert_timezone('closeddate', source_tz='America/Los_Angeles') }}          AS date_closed,
    {{ dbt_date.convert_timezone('slastartdate', source_tz='America/Los_Angeles') }}        AS date_sla_start,
    {{ dbt_date.convert_timezone('slaexitdate', source_tz='America/Los_Angeles') }}         AS date_sla_exit,
    {{ dbt_date.convert_timezone('stopstartdate', source_tz='America/Los_Angeles') }}       AS date_stop_start,
    {{ dbt_date.convert_timezone('createddate', source_tz='America/Los_Angeles') }}         AS date_reated,
    {{ dbt_date.convert_timezone('lastmodifieddate', source_tz='America/Los_Angeles') }}    AS date_last_modified,
    {{ dbt_date.convert_timezone('eventsprocesseddate', source_tz='America/Los_Angeles') }} AS date_events_processed,
    {{ dbt_date.convert_timezone('systemmodstamp', source_tz='America/Los_Angeles') }}      AS date_system_mod_stamp

  FROM stage

)

SELECT * FROM reformatted
