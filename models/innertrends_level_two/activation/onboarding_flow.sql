{{ config(
    materialized = 'table',
    alias = 'ONBOARDING_FLOW'
) }}


{% set dates = get_date_range(var('client')) %}
{% set onboarding_steps = get_onboarding_definition(var('client')) %}

WITH
STEP_ORDER AS (
    SELECT 'Created account' AS STEP, 1 AS STEP_INDEX UNION ALL
{%- for step in onboarding_steps %}
    {% set step_name = step.keys() | list | first %}
    SELECT '{{step_name}}' AS STEP, {{loop.index+1}} AS STEP_INDEX 
    {%- if not loop.last %} UNION ALL {% endif -%}
{%- endfor -%}
),

ALL_ONBOARDING_STEPS AS (
    SELECT 
        ACCOUNT_ID, 
        '' AS USER_ID, 
        CREATED_AT AS TIMESTAMP, 
        'Created account' AS ONBOARDING_STEP
    FROM
        {{ var("client") }}.ACCOUNTS
    
    UNION ALL
    
    SELECT 
        ACCOUNT_ID, 
        USER_ID, 
        TIMESTAMP, 
        ONBOARDING_STEP
    FROM
        {{ ref("onboarding_steps") }}
),

ORDERED_STEPS AS (
    SELECT
        data.ACCOUNT_ID,
        data.TIMESTAMP,
        data.ONBOARDING_STEP,
        step_order.STEP_INDEX,
        ROW_NUMBER() OVER (PARTITION BY data.ACCOUNT_ID ORDER BY data.TIMESTAMP) AS RN
    FROM ALL_ONBOARDING_STEPS data
    JOIN STEP_ORDER ON data.ONBOARDING_STEP = STEP_ORDER.STEP
),

WITH_LAG AS (
    SELECT
        *,
        LAG(ONBOARDING_STEP) OVER (PARTITION BY ACCOUNT_ID ORDER BY TIMESTAMP) AS FROM_STEP,
        LAG(TIMESTAMP) OVER (PARTITION BY ACCOUNT_ID ORDER BY TIMESTAMP) AS FROM_TIMESTAMP,
        LAG(STEP_INDEX) OVER (PARTITION BY ACCOUNT_ID ORDER BY TIMESTAMP) AS FROM_STEP_INDEX
    FROM ORDERED_STEPS
),

ALL_FUNNEL_DATA AS (
    SELECT
        ACCOUNT_ID,
        FROM_STEP,
        FROM_TIMESTAMP,
        ONBOARDING_STEP AS TO_STEP,
        TIMESTAMP AS TO_TIMESTAMP
    FROM 
        WITH_LAG
    WHERE 
        FROM_STEP_INDEX IS NOT NULL
        AND STEP_INDEX >= FROM_STEP_INDEX
    
    UNION ALL
    
    SELECT
        ACCOUNT_ID,
        ONBOARDING_STEP AS FROM_STEP,
        TIMESTAMP AS FROM_TIMESTAMP,
        '' AS TO_STEP,
        NULL AS TO_TIMESTAMP
    FROM 
        ALL_ONBOARDING_STEPS
    WHERE 
        ACCOUNT_ID NOT IN (
            SELECT DISTINCT ACCOUNT_ID 
            FROM {{ ref("onboarding_steps") }}
        )
)

SELECT
    ACCOUNT_ID,
    FROM_STEP,
    FROM_TIMESTAMP,
    TO_STEP,
    TO_TIMESTAMP
FROM
    ALL_FUNNEL_DATA
ORDER BY
    FROM_TIMESTAMP