{{ config(
    materialized = 'table',
    alias = var('table_prefix', '') ~ 'ACTIVE_ACCOUNTS'
) }}

{% set dates = get_date_range(var('client')) %}
{% set dataset = var('dataset', var('client')) %}

WITH 
    ALL_EVENTS AS (
        SELECT
            TIMESTAMP,
            EVENT,
            ACCOUNT_ID,
            USER_ID,
            EVENT_PROPERTIES
        FROM
            {{ dataset }}.EVENTS
        WHERE
            DATE(TIMESTAMP) BETWEEN '{{ dates.start_date }}' AND {{ dates.end_date }} AND (
            JSON_EXTRACT_SCALAR(EVENT_PROPERTIES, '$.ignore_retention') IS NULL
            OR JSON_EXTRACT_SCALAR(EVENT_PROPERTIES, '$.ignore_retention') != 'true')
        
        UNION ALL
        
        SELECT
            TIMESTAMP,
            EVENT,
            ACCOUNT_ID,
            USER_ID,
            EVENT_PROPERTIES
        FROM
            {{ dataset }}.UX_INTERACTIONS
        WHERE
            DATE(TIMESTAMP) BETWEEN '{{ dates.start_date }}' AND {{ dates.end_date }} AND (
            JSON_EXTRACT_SCALAR(EVENT_PROPERTIES, '$.ignore_retention') IS NULL
            OR JSON_EXTRACT_SCALAR(EVENT_PROPERTIES, '$.ignore_retention') != 'true')
        
        UNION ALL
        
        SELECT
            CREATED_AT AS TIMESTAMP,
            'Created account' AS EVENT,
            ACCOUNT_ID,
            '' AS USER_ID,
            '' AS EVENT_PROPERTIES
        FROM
            {{ dataset }}.ACCOUNTS
        WHERE
            CREATED_AT BETWEEN TIMESTAMP('{{ dates.start_date }}') AND TIMESTAMP({{ dates.end_date }})
    )

SELECT
    DATE(TIMESTAMP) AS DATE,
    ACCOUNT_ID
FROM
    ALL_EVENTS
WHERE
    ACCOUNT_ID IS NOT NULL
    AND DATE(TIMESTAMP) BETWEEN '{{ dates.start_date }}' AND {{ dates.end_date }}
GROUP BY
    DATE,
    ACCOUNT_ID