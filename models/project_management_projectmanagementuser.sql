{{ config(
    materialized='incremental',
    unique_key='external_id',
    incremental_strategy='delete+insert',
) }}

SELECT 
    DISTINCT "{{ var("table_prefix") }}_users".id::varchar as external_id,
    NOW() as created,
    NOW() as modified,
    '{{ var("timestamp") }}' as sync_timestamp,
    md5(
      '{{ var("integration_id") }}' ||
      "{{ var("table_prefix") }}_users".id ||
      'user' ||
      'monday'
    ) as id,
    'monday' as source,
    _airbyte_raw_{{ var("table_prefix") }}_users._airbyte_data as last_raw_data, 
    "{{ var("table_prefix") }}_users".name as name,
    "{{ var("table_prefix") }}_users".email as email,
    "{{ var("table_prefix") }}_users".url as url,
    CASE "{{ var("table_prefix") }}_users".is_pending WHEN true THEN 'PENDING' ELSE 'CREATED' END as status,
    NULL as firstname,
    NULL as lastname,
    "{{ var("table_prefix") }}_users".title as title,
    CASE "{{ var("table_prefix") }}_users".is_admin WHEN true THEN 'ADMIN' ELSE 'USER' END as roles,
    NULL as company_name,
    "{{ var("table_prefix") }}_users".phone as phone,
    "{{ var("table_prefix") }}_users".time_zone_identifier as timezone,
    "{{ var("table_prefix") }}_users".enabled as active,
    "{{ var("table_prefix") }}_users".photo_original as avatar,
    '{{ var("integration_id") }}'::uuid  as integration_id
FROM "{{ var("table_prefix") }}_users"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_users ON _airbyte_raw_{{ var("table_prefix") }}_users._airbyte_ab_id = "{{ var("table_prefix") }}_users"._airbyte_ab_id
