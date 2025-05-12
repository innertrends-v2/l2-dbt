{{ config(
    materialized = 'table',
    alias = 'ONBOARDING_STEPS'
) }}


{% set dates = get_date_range(var('client')) %}
{% set onboarding_steps = get_onboarding_definition(var('client')) %}

{%- set onboarding_steps_count = onboarding_steps | length %}

{%- if onboarding_steps_count > 0 %}

WITH 
    EVENTS_AND_UX AS (
        /* Combine data from EVENTS, UX_INTERACTIONS and PAYMENTS tables */
        SELECT 
            TIMESTAMP, 
            EVENT, 
            ACCOUNT_ID, 
            USER_ID, 
            EVENT_PROPERTIES
        FROM {{ var('client') }}.EVENTS
        WHERE DATE(TIMESTAMP) BETWEEN '{{ dates.start_date }}' AND {{ dates.end_date }}

        UNION ALL

        SELECT 
            TIMESTAMP, 
            EVENT, 
            ACCOUNT_ID, 
            USER_ID, 
            EVENT_PROPERTIES  
        FROM {{ var('client') }}.UX_INTERACTIONS
        WHERE DATE(TIMESTAMP) BETWEEN '{{ dates.start_date }}' AND {{ dates.end_date }}

        UNION ALL

        SELECT 
            TIMESTAMP, 
            EVENT, 
            ACCOUNT_ID, 
            USER_ID,
            PAYMENT_PROPERTIES AS EVENT_PROPERTIES
        FROM {{ var('client') }}.PAYMENTS
        WHERE DATE(TIMESTAMP) BETWEEN '{{ dates.start_date }}' AND {{ dates.end_date }}
    ),



{%- for step in onboarding_steps %}
    

    {%- set step_number = loop.index %}
    ONBOARDING_STEP_{{step_number}} AS (
        WITH
    {%- for step_name, step_definition in step.items() %}
        {%- set rule_number = step_definition["rules"] | length %}

        {%- set strict_join = '' %}
        {%- if rule_number == 2 and step_definition.get('type', '').upper() == "STRICT" %}
            {%- set strict_join = 'INNER JOIN ONBOARDING_RULE_1 ST ON E.ACCOUNT_ID = ST.ACCOUNT_ID AND E.TIMESTAMP > ST.TIMESTAMP' %}

        {% endif %}
     
        {%- for rule in step_definition["rules"] %}
            {%- set rule_number = loop.index %}
            ONBOARDING_RULE_{{rule_number}} AS (
            {%- if rule["template"] == "count" %}
                {%- if rule.get("group_by","") != "" %}
                    {%- if rule["group_by"] == "user_id" %}
                        {%- set property_to_check = 'USER_ID' %}
                    {%- else %}
                        {%- set property_to_check = "JSON_EXTRACT_SCALAR(EVENT_PROPERTIES, '$." ~ rule['group_by'] ~ "')" %}
                    {%- endif %}


                    WITH CATEGORIZED_EVENTS AS (
                        SELECT 
                            E.TIMESTAMP,
                            E.ACCOUNT_ID,
                            E.USER_ID,
                            '{{ step_name }}' AS ONBOARDING_STEP,
                            {{property_to_check}} AS PROPERTY,
                            DENSE_RANK() OVER(PARTITION BY E.ACCOUNT_ID, {{property_to_check}} ORDER BY E.TIMESTAMP ASC) AS PROPERTY_OCURRENCE
                        FROM EVENTS_AND_UX E
                        {% if loop.index == 2 %} {{strict_join}} {% endif %}
                        {% if step_definition.get("time_limit") %}
                            INNER JOIN 
                                {{ var('client') }}.ACCOUNTS TAC 
                            ON 
                                E.ACCOUNT_ID = TAC.ACCOUNT_ID
                                AND DATE(E.TIMESTAMP) <= DATE_ADD(DATE(TAC.CREATED_AT), INTERVAL {{ step_definition["time_limit"]["days_count"] }} DAY)
                        {% endif %}
                        WHERE 
                            {{ generate_activation_query(rule["content"], 'E.') }}
                            AND E.ACCOUNT_ID IN (
                            SELECT ACCOUNT_ID FROM {{ var('client') }}.ACCOUNTS
                            WHERE CREATED_AT BETWEEN TIMESTAMP('{{ dates.start_date }}') AND TIMESTAMP(CURRENT_DATE())
                            )
                    ),
                    
                    FIRST_PROPERTY_OCCURRENCES AS (
                        SELECT
                            TIMESTAMP,
                            ACCOUNT_ID,
                            USER_ID,
                            ONBOARDING_STEP,
                            PROPERTY
                        FROM CATEGORIZED_EVENTS
                        WHERE PROPERTY_OCURRENCE = 1
                    ),
                   RANKED_EVENTS AS (
                        SELECT 
                            TIMESTAMP,
                            ACCOUNT_ID,
                            USER_ID,
                            ONBOARDING_STEP,
                            ROW_NUMBER() OVER(PARTITION BY ACCOUNT_ID ORDER BY TIMESTAMP ASC) AS RN
                        FROM
                            FIRST_PROPERTY_OCCURRENCES
                    )
                    SELECT 
                        DISTINCT 
                        TIMESTAMP, 
                        ACCOUNT_ID, 
                        USER_ID, 
                        ONBOARDING_STEP
                    FROM 
                        RANKED_EVENTS
                    WHERE 
                        RN = {{ rule["value"] }}
                
                {%- else %}
                
                    WITH RANKED_EVENTS AS (
                        SELECT 
                            E.TIMESTAMP,
                            E.ACCOUNT_ID,
                            E.USER_ID,
                            '{{ step_name }}' AS ONBOARDING_STEP,
                            ROW_NUMBER() OVER(PARTITION BY E.ACCOUNT_ID ORDER BY E.TIMESTAMP ASC) AS RN
                        FROM EVENTS_AND_UX E
                        {% if loop.index == 2 %} {{strict_join}} {% endif %}
                        {% if step_definition.get("time_limit") %}
                            INNER JOIN 
                                {{ var('client') }}.ACCOUNTS TAC 
                            ON 
                                E.ACCOUNT_ID = TAC.ACCOUNT_ID
                                AND DATE(E.TIMESTAMP) <= DATE_ADD(DATE(TAC.CREATED_AT), INTERVAL {{ step_definition["time_limit"]["days_count"] }} DAY)
                        {% endif %}
                        WHERE 
                            {{ generate_activation_query(rule["content"], 'E.') }}
                            AND E.ACCOUNT_ID IN (
                            SELECT ACCOUNT_ID FROM {{ var('client') }}.ACCOUNTS
                            WHERE CREATED_AT BETWEEN TIMESTAMP('{{ dates.start_date }}') AND TIMESTAMP(CURRENT_DATE())
                            )
                    )
                    SELECT 
                        DISTINCT 
                        TIMESTAMP, 
                        ACCOUNT_ID, 
                        USER_ID, 
                        ONBOARDING_STEP
                    FROM 
                        RANKED_EVENTS
                    WHERE 
                        RN = {{ rule["value"] }}
                {%- endif %}
                
            
            {%- elif rule["template"] == "user_count" %}
                WITH RANKED_USERS AS (
                    SELECT 
                        E.ACCOUNT_ID,
                        E.USER_ID,
                        MIN(E.TIMESTAMP) AS FIRST_EVENT_TIMESTAMP,
                        '{{step_name}}' AS ONBOARDING_STEP,
                        ROW_NUMBER() OVER (PARTITION BY E.ACCOUNT_ID ORDER BY MIN(E.TIMESTAMP)) AS USER_RANK
                    FROM 
                        EVENTS_AND_UX E
                    {% if loop.index == 2 %} {{strict_join}} {% endif %}
                    {% if step_definition.get("time_limit") %}
                        INNER JOIN 
                            {{ var('client') }}.ACCOUNTS TAC 
                        ON 
                            E.ACCOUNT_ID = TAC.ACCOUNT_ID
                            AND DATE(E.TIMESTAMP) <= DATE_ADD(DATE(TAC.CREATED_AT), INTERVAL {{ step_definition["time_limit"]["days_count"] }} DAY)
                    {% endif %}
                    WHERE
                        E.USER_ID != '' AND E.USER_ID IS NOT NULL
                        AND E.ACCOUNT_ID IN
                        (
                            SELECT ACCOUNT_ID FROM {{ var('client') }}.ACCOUNTS 
                            WHERE CREATED_AT BETWEEN TIMESTAMP('{{ dates.start_date }}') AND TIMESTAMP(CURRENT_DATE())
                        )
                    GROUP BY 
                        E.ACCOUNT_ID, E.USER_ID
                )
                SELECT 
                    FIRST_EVENT_TIMESTAMP AS TIMESTAMP, 
                    ACCOUNT_ID, 
                    USER_ID, 
                    ONBOARDING_STEP
                FROM 
                    RANKED_USERS
                WHERE 
                    USER_RANK = {{ rule["value"] }}

            {%- elif rule["template"] == "days_count" %}
                WITH ACCOUNT_ACTIVITY_DAYS AS (
                    SELECT 
                        E.ACCOUNT_ID,
                        DATE(E.TIMESTAMP) AS ACTIVITY_DATE,
                        MIN(E.TIMESTAMP) AS FIRST_EVENT_TIMESTAMP
                    FROM 
                        EVENTS_AND_UX E
                    {% if loop.index == 2 %} {{strict_join}} {% endif %}
                    {% if step_definition.get("time_limit") %}
                        INNER JOIN 
                            {{ var('client') }}.ACCOUNTS TAC 
                        ON 
                            E.ACCOUNT_ID = TAC.ACCOUNT_ID
                            AND DATE(E.TIMESTAMP) >= DATE_ADD(DATE(TAC.CREATED_AT), INTERVAL {{ step_definition["time_limit"]["days_count"] }} DAY)
                    {% endif %}
                    WHERE
                        {% if "content" in rule %}
                            {{ generate_activation_query(rule["content"], 'E.') }}
                            AND 
                        {% endif %}
                        E.ACCOUNT_ID IN
                        (
                            SELECT ACCOUNT_ID FROM {{ var('client') }}.ACCOUNTS 
                            WHERE CREATED_AT BETWEEN TIMESTAMP('{{ dates.start_date }}') AND TIMESTAMP(CURRENT_DATE())
                        )
                    GROUP BY 
                        E.ACCOUNT_ID, ACTIVITY_DATE
                ),
                RANKED_ACTIVITY_DAYS AS (
                    SELECT 
                        ACCOUNT_ID,
                        ACTIVITY_DATE,
                        FIRST_EVENT_TIMESTAMP,
                        ROW_NUMBER() OVER (PARTITION BY ACCOUNT_ID ORDER BY ACTIVITY_DATE) AS DAY_RANK
                    FROM 
                        ACCOUNT_ACTIVITY_DAYS
                )
                SELECT 
                    FIRST_EVENT_TIMESTAMP AS TIMESTAMP,
                    ACCOUNT_ID,
                    '' AS USER_ID,
                    '{{step_name}}' AS ONBOARDING_STEP
                FROM 
                    RANKED_ACTIVITY_DAYS
                WHERE 
                    DAY_RANK = {{ rule["value"] }}
                GROUP BY 
                    ACCOUNT_ID, TIMESTAMP
            {%- endif %}

             ){%- if not loop.last %} , {% endif -%}
             
        {%- endfor -%}
       
        
        {%- if rule_number == 1 %}
            SELECT * FROM ONBOARDING_RULE_1 
        {%- elif rule_number > 1 %}
            ,
            COMBINED_ONBOARDING_RULES AS
            (
            {%- for i in range(1, rule_number + 1) %}
                SELECT ACCOUNT_ID, USER_ID, TIMESTAMP, '{{ i }}' AS RULE_NUMBER
                FROM ONBOARDING_RULE_{{ i }}
                {%- if not loop.last %} UNION ALL {% endif %}
            {%- endfor %}
            )

            {% if step_definition.get('type', '').upper() == 'ANY' %}
                SELECT 
                    MIN(TIMESTAMP) AS TIMESTAMP,
                    ACCOUNT_ID, 
                    USER_ID,
                    '{{step_name}}' AS ONBOARDING_STEP
                FROM 
                    COMBINED_ONBOARDING_RULES
                GROUP BY 
                    ACCOUNT_ID, USER_ID
            {% else %}
                SELECT 
                    MAX(TIMESTAMP) AS TIMESTAMP,
                    ACCOUNT_ID, 
                    USER_ID,
                    '{{step_name}}' AS ONBOARDING_STEP
                FROM 
                    COMBINED_ONBOARDING_RULES
                WHERE
                    ACCOUNT_ID IN (
                        SELECT 
                            ACCOUNT_ID
                        FROM 
                            COMBINED_ONBOARDING_RULES
                        GROUP BY 
                            ACCOUNT_ID
                        HAVING 
                            COUNT(DISTINCT RULE_NUMBER) = {{rule_number}}
                    )
                GROUP BY 
                    ACCOUNT_ID, USER_ID

            {% endif %}
        {% endif %}
        
    {%- endfor -%} 
    

    ),
{%- endfor -%}
-- completed the loop for all the steps

ALL_ONBOARDING_STEPS AS (
    {%- for i in range(1, onboarding_steps_count + 1) %}
        SELECT ACCOUNT_ID, USER_ID, TIMESTAMP, ONBOARDING_STEP
        FROM ONBOARDING_STEP_{{ i }}
        {%- if not loop.last %} UNION ALL {% endif %}
    {%- endfor -%}
)

SELECT
    os.ACCOUNT_ID, 
    os.USER_ID, 
    os.TIMESTAMP, 
    os.ONBOARDING_STEP
FROM
    ALL_ONBOARDING_STEPS os
INNER JOIN
    {{ var("client") }}.ACCOUNTS ac
ON 
    ac.ACCOUNT_ID = os.ACCOUNT_ID AND ac.CREATED_AT <= os.TIMESTAMP --consider only onboarding steps that happen after account creation
ORDER BY
    TIMESTAMP ASC

{% else %}

SELECT 
    NULL AS ACCOUNT_ID,
    NULL AS USER_ID,
    NULL AS TIMESTAMP,
    NULL AS ONBOARDING_STEP
FROM (SELECT 1) 
WHERE FALSE

{% endif %}
