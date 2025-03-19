{{ config(
    materialized = 'table',
    alias = 'ACTIVE_ACCOUNTS'
) }}


WITH date_settings AS (
    SELECT
        start_date,
        date_sub(current_date(), INTERVAL 1 DAY) AS end_date
    FROM
        DATA_SETTINGS.{{ var('client') }}
),

all_events AS (
    SELECT
        timestamp,
        event,
        account_id,
        user_id,
        event_properties
    FROM
        {{ var('client') }}.EVENTS
    WHERE
        json_extract_scalar(event_properties, '$.ignore_retention') IS NULL
        OR json_extract_scalar(event_properties, '$.ignore_retention') != 'true'
    UNION ALL
    SELECT
        timestamp,
        event,
        account_id,
        user_id,
        event_properties
    FROM
        {{ var('client') }}.UX_INTERACTIONS
    WHERE
        json_extract_scalar(event_properties, '$.ignore_retention') IS NULL
        OR json_extract_scalar(event_properties, '$.ignore_retention') != 'true'
    UNION ALL
    SELECT
        created_at AS timestamp,
        'Created account' AS event,
        account_id,
        '' AS user_id,
        '' AS event_properties
    FROM
        {{ var('client') }}.ACCOUNTS
)
SELECT
    date(timestamp) AS DATE,
    ACCOUNT_ID
FROM
    all_events,
    date_settings
WHERE
    account_id IS NOT NULL
    AND date(timestamp) BETWEEN date_settings.start_date AND date_settings.end_date
GROUP BY
    date,
    account_id