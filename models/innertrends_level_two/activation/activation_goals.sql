{{ config(
    materialized = 'table',
    alias = 'ACTIVATION_GOALS'
) }}

{% set dates = get_date_range(var('client')) %}
{% set activation_goals = get_activation_goals_definition(var('client')) %}

--{{activation_goals}}

{%- set activation_goals_count = activation_goals | length %}
{%- if activation_goals_count > 0 %}

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

        UNION ALL

        SELECT 
            TIMESTAMP, 
            EVENT, 
            ACCOUNT_ID, 
            USER_ID, 
            EVENT_PROPERTIES  
        FROM {{ var('client') }}.UX_INTERACTIONS

        UNION ALL

        SELECT 
            TIMESTAMP, 
            EVENT, 
            ACCOUNT_ID, 
            USER_ID,
            PAYMENT_PROPERTIES AS EVENT_PROPERTIES
        FROM 
            {{ var('client') }}.PAYMENTS
    ),


{%- for goal in activation_goals %}
    
    {%- set goal_number = loop.index %}
    GOAL_{{goal_number}} AS (
        WITH
    {%- for goal_name, goal_definition in goal.items() %}
        {%- set rule_number = goal_definition["rules"] | length %}
        
        {%- set strict_join = '' %}
        {%- if rule_number == 2 and goal_definition.get('type', '').upper() == "STRICT" %}
            {%- set strict_join = 'INNER JOIN GOAL_RULES_1 ST ON E.ACCOUNT_ID = ST.ACCOUNT_ID AND E.TIMESTAMP > ST.TIMESTAMP' %}

        {% endif %}
    
        {%- for rule in goal_definition["rules"] %}
            {%- set rule_number = loop.index %}
            GOAL_RULES_{{rule_number}} AS (
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
                            '{{ goal_name }}' AS GOAL,
                            {{property_to_check}} AS PROPERTY,
                            DENSE_RANK() OVER(PARTITION BY E.ACCOUNT_ID, {{property_to_check}} ORDER BY E.TIMESTAMP ASC) AS PROPERTY_OCURRENCE
                        FROM EVENTS_AND_UX E
                        {% if loop.index == 2 %} {{strict_join}} {% endif %}
                        {% if goal_definition.get("time_limit") %}
                            INNER JOIN 
                                {{ var('client') }}.ACCOUNTS TAC 
                            ON 
                                E.ACCOUNT_ID = TAC.ACCOUNT_ID
                                AND DATE(E.TIMESTAMP) <= DATE_ADD(DATE(TAC.CREATED_AT), INTERVAL {{ goal_definition["time_limit"]["days_count"] }} DAY)
                        {% endif %}
                        WHERE {{ generate_activation_query(rule["content"], 'E.') }}
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
                            GOAL,
                            PROPERTY
                        FROM CATEGORIZED_EVENTS
                        WHERE PROPERTY_OCURRENCE = 1
                    ),
                   RANKED_EVENTS AS (
                        SELECT 
                            TIMESTAMP,
                            ACCOUNT_ID,
                            USER_ID,
                            GOAL,
                            ROW_NUMBER() OVER(PARTITION BY ACCOUNT_ID ORDER BY TIMESTAMP ASC) AS RN
                        FROM
                            FIRST_PROPERTY_OCCURRENCES
                    )
                    SELECT 
                        DISTINCT 
                        TIMESTAMP, 
                        ACCOUNT_ID, 
                        USER_ID, 
                        GOAL
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
                            '{{ goal_name }}' AS GOAL,
                            ROW_NUMBER() OVER(PARTITION BY E.ACCOUNT_ID ORDER BY E.TIMESTAMP ASC) AS RN
                        FROM EVENTS_AND_UX E
                        {% if loop.index == 2 %} {{strict_join}} {% endif %}
                        {% if goal_definition.get("time_limit") %}
                            INNER JOIN 
                                {{ var('client') }}.ACCOUNTS TAC 
                            ON 
                                E.ACCOUNT_ID = TAC.ACCOUNT_ID
                                AND DATE(E.TIMESTAMP) <= DATE_ADD(DATE(TAC.CREATED_AT), INTERVAL {{ goal_definition["time_limit"]["days_count"] }} DAY)
                        {% endif %}
                        WHERE {{ generate_activation_query(rule["content"], 'E.') }}
                            AND E.ACCOUNT_ID IN (
                                SELECT ACCOUNT_ID FROM {{ var('client') }}.ACCOUNTS
                                WHERE CREATED_AT BETWEEN TIMESTAMP('{{ dates.start_date }}') AND TIMESTAMP(CURRENT_DATE())
                            )

                        
                    )
                    SELECT 
                        DISTINCT TIMESTAMP,
                        ACCOUNT_ID, 
                        USER_ID, 
                        GOAL
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
                        '{{goal_name}}' AS GOAL,
                        ROW_NUMBER() OVER (PARTITION BY E.ACCOUNT_ID ORDER BY MIN(E.TIMESTAMP)) AS USER_RANK
                    FROM 
                        EVENTS_AND_UX E
                    {% if loop.index == 2 %} {{strict_join}} {% endif %}
                    {% if goal_definition.get("time_limit") %}
                        INNER JOIN 
                            {{ var('client') }}.ACCOUNTS TAC 
                        ON 
                            E.ACCOUNT_ID = TAC.ACCOUNT_ID
                            AND DATE(E.TIMESTAMP) <= DATE_ADD(DATE(TAC.CREATED_AT), INTERVAL {{ goal_definition["time_limit"]["days_count"] }} DAY)
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
                    GOAL
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
                    {% if goal_definition.get("time_limit") %}
                        INNER JOIN 
                            {{ var('client') }}.ACCOUNTS TAC 
                        ON 
                            E.ACCOUNT_ID = TAC.ACCOUNT_ID
                            AND DATE(E.TIMESTAMP) <= DATE_ADD(DATE(TAC.CREATED_AT), INTERVAL {{ goal_definition["time_limit"]["days_count"] }} DAY)
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
                    '{{goal_name}}' AS GOAL
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
            SELECT * FROM GOAL_RULES_1 
        {%- elif rule_number > 1 %}
            ,
            COMBINED_GOAL_RULES AS
            (
            {%- for i in range(1, rule_number + 1) %}
                SELECT ACCOUNT_ID, USER_ID, TIMESTAMP, '{{ i }}' AS RULE_NUMBER
                FROM GOAL_RULES_{{ i }}
                {%- if not loop.last %} UNION ALL {% endif %}
            {%- endfor %}
            )

            {% if goal_definition.get('type', '').upper() == 'ANY' %}
                SELECT 
                    MIN(TIMESTAMP) AS TIMESTAMP,
                    ACCOUNT_ID, 
                    USER_ID,
                    '{{goal_name}}' AS GOAL
                FROM 
                    COMBINED_GOAL_RULES
                GROUP BY 
                    ACCOUNT_ID, USER_ID
            {% else %}
                SELECT 
                    MAX(TIMESTAMP) AS TIMESTAMP,
                    ACCOUNT_ID, 
                    USER_ID,
                    '{{goal_name}}' AS GOAL
                FROM 
                    COMBINED_GOAL_RULES
                WHERE
                    ACCOUNT_ID IN (
                        SELECT 
                            ACCOUNT_ID
                        FROM 
                            COMBINED_GOAL_RULES
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

ALL_GOALS AS (
    {%- for i in range(1, activation_goals_count + 1) %}
        SELECT ACCOUNT_ID, USER_ID, TIMESTAMP, GOAL
        FROM GOAL_{{ i }}
        {%- if not loop.last %} UNION ALL {% endif %}
    {%- endfor -%}
)

SELECT
    ACCOUNT_ID, 
    USER_ID, 
    TIMESTAMP, 
    GOAL
FROM
    ALL_GOALS
ORDER BY
    TIMESTAMP ASC

{% else %}

SELECT 
    NULL AS ACCOUNT_ID,
    NULL AS USER_ID,
    NULL AS TIMESTAMP,
    NULL AS GOAL
FROM (SELECT 1) 
WHERE FALSE

{% endif %}