{{ config(
    materialized = 'table',
    alias = 'ACTIVATION_GOALS'
) }}

{% set dates = get_date_range(var('client')) %}
{% set activation_goals = get_activation_goals_definition(var('client')) %}

--{{activation_goals}}

WITH 
    EVENTS_AND_UX AS (
        /* Combine data from EVENTS,  UX_INTERACTIONS and PAYMENTS tables */
        SELECT 
            timestamp, 
            event, 
            account_id, 
            user_id, 
            event_properties
        FROM {{ var('client') }}.EVENTS

        UNION ALL

        SELECT 
            timestamp, 
            event, 
            account_id, 
            user_id, 
            event_properties  
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


{%- set activation_goals_count = activation_goals | length %}
{%- for goal in activation_goals %}
    
    {%- set goal_number = loop.index %}
    goal_{{goal_number}} AS (
        WITH
    {%- for goal_name, goal_definition in goal.items() %}
        {%- set rule_number = goal_definition["rules"] | length %}

        {%- set strict_join = '' %}
        {%- if rule_number == 2 and goal_definition["type"].upper() == "STRICT" %}
            {%- set strict_join = 'INNER JOIN goal_rules_1 ST ON E.ACCOUNT_ID = ST.ACCOUNT_ID AND E.TIMESTAMP > ST.TIMESTAMP' %}

        {% endif %}
    
        {%- for rule in goal_definition["rules"] %}
            {%- set rule_number = loop.index %}
            goal_rules_{{rule_number}} AS (
            {%- if rule["template"] == "count" %}
                
                WITH RankedEvents AS (
                    SELECT 
                        {%- if rule.get("group_by") == "user_id" %} MIN(E.TIMESTAMP) {%- else %} E.TIMESTAMP {%- endif %} AS TIMESTAMP,
                        E.ACCOUNT_ID,
                        E.USER_ID,
                        '{{ goal_name }}' AS GOAL,
                        ROW_NUMBER() OVER(PARTITION BY E.ACCOUNT_ID ORDER BY E.TIMESTAMP ASC) as RN
                    FROM EVENTS_AND_UX E
                    {{strict_join}}
                    {% if goal_definition.get("time_limit") %}
                        INNER JOIN 
                            {{ var('client') }}.ACCOUNTS TAC 
                        ON 
                            E.ACCOUNT_ID = TAC.ACCOUNT_ID
                            AND DATE(E.TIMESTAMP) <= DATE_ADD(DATE(TAC.CREATED_AT), INTERVAL {{ step_definition["time_limit"]["days_count"] }} DAY)
                    {% endif %}
                    WHERE {{ generate_activation_query(rule["content"], 'E.') }}
                        AND E.ACCOUNT_ID IN (
                        SELECT ACCOUNT_ID FROM {{ var('client') }}.ACCOUNTS
                        WHERE CREATED_AT BETWEEN TIMESTAMP('{{ dates.start_date }}') AND TIMESTAMP(CURRENT_DATE())
                        )

                    {%- if rule.get("group_by") == "user_id" %} GROUP BY E.ACCOUNT_ID, E.USER_ID {%- endif %}
                )
                SELECT 
                    DISTINCT 
                    TIMESTAMP, 
                    ACCOUNT_ID, 
                    USER_ID, 
                    GOAL
                FROM 
                    RankedEvents
                WHERE 
                    RN = {{ rule["value"] }}
                
            
            {%- elif rule["template"] == "user_count" %}
                WITH RankedUsers AS (
                    SELECT 
                        E.ACCOUNT_ID,
                        E.USER_ID,
                        MIN(E.TIMESTAMP) as FirstEventTimestamp,
                        '{{goal_name}}' as GOAL,
                        ROW_NUMBER() OVER (PARTITION BY E.ACCOUNT_ID ORDER BY MIN(E.TIMESTAMP)) as UserRank
                    FROM 
                        EVENTS_AND_UX E
                    {{ strict_join }}
                    {% if goal_definition.get("time_limit") %}
                        INNER JOIN 
                            {{ var('client') }}.ACCOUNTS TAC 
                        ON 
                            E.ACCOUNT_ID = TAC.ACCOUNT_ID
                            AND DATE(E.TIMESTAMP) <= DATE_ADD(DATE(TAC.CREATED_AT), INTERVAL {{ step_definition["time_limit"]["days_count"] }} DAY)
                    {% endif %}
                    WHERE
                        E.USER_ID != '' and E.USER_ID is not NULL
                        AND E.ACCOUNT_ID in
                        (
                            SELECT ACCOUNT_ID FROM {{ var('client') }}.ACCOUNTS 
                            WHERE CREATED_AT BETWEEN TIMESTAMP('{{ dates.start_date }}') AND TIMESTAMP(CURRENT_DATE())
                        )
                    GROUP BY 
                        E.ACCOUNT_ID, E.USER_ID
                )
                SELECT 
                    FirstEventTimestamp as TIMESTAMP, 
                    ACCOUNT_ID, 
                    USER_ID, 
                    GOAL
                FROM 
                    RankedUsers
                WHERE 
                    UserRank = {{ rule["value"] }}

            {%- elif rule["template"] == "days_count" %}
                WITH AccountActivityDays AS (
                    SELECT 
                        E.ACCOUNT_ID,
                        DATE(E.TIMESTAMP) AS ActivityDate,
                        MIN(E.TIMESTAMP) AS FirstEventTimestamp
                    FROM 
                        EVENTS_AND_UX E
                    {{strict_join}}
                    {% if goal_definition.get("time_limit") %}
                        INNER JOIN 
                            {{ var('client') }}.ACCOUNTS TAC 
                        ON 
                            E.ACCOUNT_ID = TAC.ACCOUNT_ID
                            AND DATE(E.TIMESTAMP) <= DATE_ADD(DATE(TAC.CREATED_AT), INTERVAL {{ step_definition["time_limit"]["days_count"] }} DAY)
                    {% endif %}
                    WHERE
                        {% if "content" in rule %}
                            {{ generate_activation_query(rule["content"], 'E.') }}
                            AND 
                        {% endif %}
                        E.ACCOUNT_ID in
                        (
                            SELECT ACCOUNT_ID FROM {{ var('client') }}.ACCOUNTS 
                            WHERE CREATED_AT BETWEEN TIMESTAMP('{{ dates.start_date }}') AND TIMESTAMP(CURRENT_DATE())
                        )
                        {strict_condition}
                    GROUP BY 
                        E.ACCOUNT_ID, ActivityDate
                ),
                RankedActivityDays AS (
                    SELECT 
                        ACCOUNT_ID,
                        ActivityDate,
                        FirstEventTimestamp,
                        ROW_NUMBER() OVER (PARTITION BY ACCOUNT_ID ORDER BY ActivityDate) AS DayRank
                    FROM 
                        AccountActivityDays
                )
                SELECT 
                    FirstEventTimestamp as TIMESTAMP,
                    ACCOUNT_ID,
                    '' as USER_ID,
                    '{{goal_name}}' as GOAL
                FROM 
                    RankedActivityDays
                WHERE 
                    DayRank = {{ rule["value"] }}
                GROUP BY 
                    ACCOUNT_ID, TIMESTAMP
            {%- endif %}

             ){%- if not loop.last %} , {% endif -%}
             
        {%- endfor -%}
       
        
        {%- if rule_number == 1 %}
            SELECT * FROM goal_rules_1 
        {%- elif rule_number > 1 %}
            ,
            combined_goal_rules AS
            (
            {%- for i in range(1, rule_number + 1) %}
                SELECT ACCOUNT_ID, USER_ID, TIMESTAMP, '{{ i }}' AS RULE_NUMBER
                FROM goal_rules_{{ i }}
                {%- if not loop.last %} UNION ALL {% endif %}
            {%- endfor %}
            )

            {% if goal_definition.get('type', '').upper() == 'ANY' %}
                SELECT 
                    MIN(TIMESTAMP) as TIMESTAMP,
                    ACCOUNT_ID, 
                    USER_ID,
                    '{{goal_name}}' as GOAL
                FROM 
                    combined_onboarding_rules
                GROUP BY 
                    ACCOUNT_ID, USER_ID
            {% else %}
                SELECT 
                    MAX(TIMESTAMP) as TIMESTAMP,
                    ACCOUNT_ID, 
                    USER_ID,
                    '{{goal_name}}' as GOAL
                FROM 
                    combined_onboarding_rules
                WHERE
                    ACCOUNT_ID IN (
                        SELECT 
                            ACCOUNT_ID
                        FROM 
                            combined_onboarding_rules
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

all_goals AS (
    {%- for i in range(1, activation_goals_count + 1) %}
        SELECT ACCOUNT_ID, USER_ID, TIMESTAMP, GOAL
        FROM goal_{{ i }}
        {%- if not loop.last %} UNION ALL {% endif %}
    {%- endfor -%}
)

SELECT
    ACCOUNT_ID, 
    USER_ID, 
    TIMESTAMP, 
    GOAL
FROM
    all_goals
ORDER BY
    TIMESTAMP ASC