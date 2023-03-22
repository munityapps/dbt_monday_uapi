{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
)}}

SELECT
    md5(
        '{{ var("integration_id") }}'::text ||
        project.id::text ||
        "group"."id"::text ||
        'group'::text ||
        'monday'::text
    ) as id,
    "group"."id" as external_id,
    'monday' as source,
    NOW() as created,
    NOW() as modified,
    '{{ var("integration_id") }}'::uuid as integration_id,
    "group".raw as last_raw_data,
    NULL as url,
    "group".title as name,
    "group".deleted::boolean as deleted,
    "group".archived::boolean as archived,
    project.id as project_id,
    '{{ var("timestamp") }}' as sync_timestamp
FROM (SELECT
        groups,
        id as board_id,
        jsonb_array_elements(groups) as raw,
        jsonb_array_elements(groups)->>'id' as id,
        jsonb_array_elements(groups)->>'color' as color,
        jsonb_array_elements(groups)->>'title' as title,
        jsonb_array_elements(groups)->>'deleted' as deleted,
        jsonb_array_elements(groups)->>'archived' as archived,
        jsonb_array_elements(groups)->>'position' as position
    FROM "{{ var("table_prefix") }}_boards"
) as "group"
LEFT JOIN {{ ref('project_management_projectmanagementproject') }} AS project
ON  project.external_id = "group".board_id
