{{ config(
    materialized='incremental',
    unique_key='external_id',
    incremental_strategy='delete+insert',
) }}

SELECT 
    DISTINCT "{{ var("table_prefix") }}_boards".id as external_id,
    NOW() as created,
    NOW() as modified,
    '{{ var("timestamp") }}' as sync_timestamp,
    md5(
      "{{ var("table_prefix") }}_boards".id ||
      'project' ||
      'monday' ||
      '{{ var("integration_id") }}'
    )  as id,
    'monday' as source,
    "{{ var("table_prefix") }}_boards".name as name,
    NULL as folder,
    'https://app.monday.com/boards/' || "{{ var("table_prefix") }}_boards".id as url,
    NULL as status,
    CASE "{{ var("table_prefix") }}_boards".board_kind WHEN 'public' THEN false ELSE true END as private,
    "{{ var("table_prefix") }}_boards".description as description,
    NULL::date as creation_date,
    NULL::date as begin_date,
    NULL::date as end_date,
    owner.id as owner_id, 
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_boards._airbyte_data as last_raw_data 
FROM "{{ var("table_prefix") }}_boards"
    left join {{ ref('project_management_projectmanagementuser') }} as owner
        on owner.external_id = "{{ var("table_prefix") }}_boards".owner->>'id' and owner.source = 'monday' and owner.integration_id = '{{ var("integration_id") }}'
    left join _airbyte_raw_{{ var("table_prefix") }}_boards
        on _airbyte_raw_{{ var("table_prefix") }}_boards._airbyte_ab_id = "{{ var("table_prefix") }}_boards"._airbyte_ab_id
