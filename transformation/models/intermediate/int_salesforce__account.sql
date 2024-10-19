WITH stage AS (

  SELECT * FROM {{ ref('stg_salesforce__account') }}

),

reformatted AS (

  SELECT

--keys
    account_id,
    parentid         AS account_parent__account_id,
    ownerid          AS account_owner__user_id,
    createdbyid      AS account_created_by__user_id,
    lastmodifiedbyid AS account_last_modified_by__user_id,

--direct fields
    phone,
    fax,
    website,
    sic,
    industry,
    jigsaw,

-- renamed fields
    name                    AS account_name,
    type                    AS account_type,
    billingstreet           AS billing_street,
    billingcity             AS billing_city,
    billingstate            AS billing_state,
    billingpostalcode       AS billing_postal_code,
    billingcountry          AS billing_country,
    billinglatitude         AS billing_latitude,
    billinglongitude        AS billing_longitude,
    billinggeocodeaccuracy  AS billing_geocode_accuracy,
    shippingstreet          AS shipping_street,
    shippingcity            AS shipping_city,
    shippingstate           AS shipping_state,
    shippingpostalcode      AS shipping_postal_code,
    shippingcountry         AS shipping_country,
    shippinglatitude        AS shipping_latitude,
    shippinglongitude       AS shipping_longitude,
    shippinggeocodeaccuracy AS shipping_geocode_accuracy,
    accountnumber           AS account_number,
    annualrevenue           AS annual_revenue,
    numberofemployees       AS number_of_employees,
    ownership               AS account_ownership,
    tickersymbol            AS ticker_symbol,
    description             AS account_description,
    rating                  AS account_rating,
    site                    AS account_site,
    cleanstatus             AS clean_status,
    accountsource           AS account_source,
    dunsnumber              AS duns_number,
    tradestyle              AS trade_style,
    naicscode               AS naics_code,
    naicsdesc               AS naics_desc,
    yearstarted             AS year_started,
    sicdesc                 AS sic_desc,
    customerpriority__c     AS customer_priority,
    sla__c                  AS sla,
    numberoflocations__c    AS number_of_locations,
    upsellopportunity__c    AS upsell_opportunity,
    slaserialnumber__c      AS sla_serial_number,

-- boolean conversions
    {{ convert_to_boolean('isdeleted') }}              AS is_account_deleted,
    {{ convert_to_boolean('active__c', 'yes', 'no') }} AS is_account_active,

-- date conversions
    {{ dbt_date.convert_timezone('createddate', source_tz='America/Los_Angeles') }}          AS date_account_created,
    {{ dbt_date.convert_timezone('lastmodifieddate', source_tz='America/Los_Angeles') }}     AS date_account_last_modified,
    {{ dbt_date.convert_timezone('lastactivitydate', source_tz='America/Los_Angeles') }}     AS date_account_last_activity,
    {{ dbt_date.convert_timezone('slaexpirationdate__c', source_tz='America/Los_Angeles') }} AS date_account_sla_expiration,
    {{ dbt_date.convert_timezone('systemmodstamp', source_tz='America/Los_Angeles') }}       AS date_account_system_mod_stamp

  FROM stage

)

SELECT * FROM reformatted
