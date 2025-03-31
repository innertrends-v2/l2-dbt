{{ config(
    materialized = 'table',
    alias = 'ACTIVE_ACCOUNTS_LAST_SEEN'
) }}

{% set dates = get_date_range(var('client')) %}


SELECT 
    ACCOUNT_ID, 
    MAX(DATE) AS LAST_SEEN
FROM 
    {{ ref('active_accounts') }}
WHERE 
    DATE >= '{{ dates.start_date }}'
    AND DATE <= {{ dates.end_date }}
GROUP BY 
    ACCOUNT_ID

