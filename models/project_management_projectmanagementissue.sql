{{ config(
    materialized='incremental',
    unique_key='external_id',
    incremental_strategy='delete+insert',
)}}

SELECT 
    DISTINCT "{{ var("table_prefix") }}_items".id as external_id,
    NOW() as created,
    NOW() as modified,
    md5(
      '{{ var("integration_id") }}' ||
      project.id ||
      "{{ var("table_prefix") }}_items".id ||
      'task' ||
      'monday'
    )  as id,
    'monday' as source,
    '{{ var("integration_id") }}'::uuid as integration_id,
    '{{ var("timestamp") }}' as sync_timestamp,
    _airbyte_raw_{{ var("table_prefix") }}_items._airbyte_data as last_raw_data, 
    'https://app.monday.com/boards/'::varchar || ("{{ var("table_prefix") }}_items".board->>'id') || '/pulses/' || "{{ var("table_prefix") }}_items".id as url,
    NULL as priority,
    NULL as severity,
    "{{ var("table_prefix") }}_items".name,
    '' as description,
    NULL::date as due_date,
    NULL::boolean as complete,
    NULL as tags,
    assignee.id as assignee_id,
    NULL as creator_id,
    project.id as project_id,
    NULL as status_id,
    type.id as type_id,
    "group".id as group_id,
    false as is_milestone
FROM "{{ var("table_prefix") }}_items"
    LEFT JOIN {{ ref('project_management_projectmanagementproject') }} as project
        on project.external_id = "{{ var("table_prefix") }}_items".board->>'id'
        and project.source = 'monday'
        and project.integration_id = '{{ var("integration_id") }}'
    LEFT JOIN {{ ref('project_management_projectmanagementuser') }} as assignee
        on "{{ var("table_prefix") }}_items".subscribers->0->>'id' = assignee.external_id
        and assignee.source = 'monday'
        and assignee.integration_id = '{{ var("integration_id") }}'
    LEFT JOIN {{ ref('project_management_projectmanagementissuetype') }} as type 
        ON type.external_id = 'task'
        AND type.integration_id = '{{ var("integration_id") }}'
        AND type.project_id = project.id
    LEFT JOIN {{ ref('project_management_projectmanagementgroup') }} as "group"
        on "{{ var("table_prefix") }}_items".group->>'id' = "group".external_id
        and assignee.source = 'monday'
        and assignee.integration_id = '{{ var("integration_id") }}'
    LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_items
        on _airbyte_raw_{{ var("table_prefix") }}_items._airbyte_ab_id = "{{ var("table_prefix") }}_items"._airbyte_ab_id
