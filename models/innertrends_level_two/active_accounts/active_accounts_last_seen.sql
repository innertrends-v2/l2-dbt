{{ config(
    materialized = 'table',
    alias = 'ACTIVE_ACCOUNTS_LAST_SEEN'
) }}



WITH date_settings AS (
    SELECT
        start_date,
        date_sub(current_date(), INTERVAL 1 DAY) AS end_date
    FROM
        DATA_SETTINGS.{{ var('client') }}
)
SELECT 
    ACCOUNT_ID, 
    MAX(DATE) AS LAST_SEEN
FROM 
    {{ ref('active_accounts') }},
    DATE_SETTINGS
WHERE 
    DATE >= CAST(START_DATE AS DATETIME)
    AND DATE <= CAST(END_DATE AS DATETIME)
GROUP BY 
    ACCOUNT_ID

