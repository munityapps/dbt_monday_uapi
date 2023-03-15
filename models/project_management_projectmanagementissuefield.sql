{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
)}}

SELECT
    md5(
        '{{ var("integration_id") }}'::text ||
        board.board_id::text ||
        'task'::text ||
        board.id ||
        'issuefield'::text ||
        'monday'::text
    ) as id,
    board.id as external_id,
    'monday' as source,
    NOW() as created,
    NOW() as modified,
    '{{ var("integration_id") }}'::uuid as integration_id,
    board.raw as last_raw_data,
    board.title as name,
    NULL as description,
    board.type as type,
    board.id as path,
    project.id as project_id,
    '{{ var("timestamp") }}' as sync_timestamp,
    type.id as issue_type_id
FROM (SELECT
        columns,
        id as board_id,
        jsonb_array_elements(columns) as raw,
        jsonb_array_elements(columns)->>'id' as id,
        jsonb_array_elements(columns)->>'type' as type,
        jsonb_array_elements(columns)->>'title' as title,
        jsonb_array_elements(columns)->>'width' as width,
        jsonb_array_elements(columns)->>'archived' as archived,
        jsonb_array_elements(columns)->>'settings_str' as settings_str
    FROM "{{ var("table_prefix") }}_boards"
) as board
LEFT JOIN {{ ref('project_management_projectmanagementproject') }} AS project
ON  project.external_id = board.board_id
LEFT JOIN {{ ref('project_management_projectmanagementissuetype') }} AS type
ON  type.project_id = project.id