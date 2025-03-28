{{ config(
    materialized = 'table',
    alias = 'ONBOARDING_FLOW'
) }}


{% set dates = get_date_range(var('client')) %}
{% set onboarding_steps = get_onboarding_definition(var('client')) %}

WITH
step_order as (
    SELECT 'Created account' as step, 1 as step_index UNION ALL
{%- for step in onboarding_steps %}
    {% set step_name = step.keys() | list | first %}
    SELECT '{{step_name}}' as step, {{loop.index+1}} as step_index 
    {%- if not loop.last %} UNION ALL {% endif -%}
{%- endfor -%}
),

all_onboarding_steps as
(
    SELECT 
        ACCOUNT_ID, 
        '' as USER_ID, 
        CREATED_AT as TIMESTAMP, 
        'Created account' as ONBOARDING_STEP
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

ordered_steps as (
  select
    data.ACCOUNT_ID,
    data.TIMESTAMP,
    data.ONBOARDING_STEP,
    step_order.step_index,
    row_number() over (partition by data.ACCOUNT_ID order by data.TIMESTAMP) as rn
  from all_onboarding_steps data
  join step_order on data.ONBOARDING_STEP = step_order.step
),

with_lag as (
  select
    *,
    lag(ONBOARDING_STEP) over (partition by ACCOUNT_ID order by TIMESTAMP) as FROM_STEP,
    lag(TIMESTAMP) over (partition by ACCOUNT_ID order by TIMESTAMP) as FROM_TIMESTAMP,
    lag(step_index) over (partition by ACCOUNT_ID order by TIMESTAMP) as from_step_index
  from ordered_steps
),

all_funnel_data as
(
SELECT
    ACCOUNT_ID,
    FROM_STEP,
    FROM_TIMESTAMP,
    ONBOARDING_STEP as TO_STEP,
    TIMESTAMP as TO_TIMESTAMP
FROM 
    with_lag
WHERE 
    from_step_index is not null
    and step_index >= from_step_index
UNION ALL
SELECT
    ACCOUNT_ID,
    ONBOARDING_STEP as FROM_STEP,
    TIMESTAMP as FROM_TIMESTAMP,
    '' as TO_STEP,
    NULL as TO_TIMESTAMP
FROM 
    all_onboarding_steps
WHERE 
    ACCOUNT_ID not in 
    (SELECT DISTINCT ACCOUNT_ID FROM {{ ref("onboarding_steps") }})
)

SELECT
    ACCOUNT_ID,
    FROM_STEP,
    FROM_TIMESTAMP,
    TO_STEP,
    TO_TIMESTAMP
FROM
    all_funnel_data
ORDER BY
    FROM_TIMESTAMP 