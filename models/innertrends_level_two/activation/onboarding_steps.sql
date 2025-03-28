{{ config(
    materialized = 'table',
    alias = 'ONBOARDING_STEPS'
) }}


{% set dates = get_date_range(var('client')) %}
{% set onboarding_steps = get_onboarding_definition(var('client')) %}

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


{%- set onboarding_steps_count = onboarding_steps | length %}
{%- for step in onboarding_steps %}
    

    {%- set step_number = loop.index %}
    onboarding_step_{{step_number}} AS (
        WITH
    {%- for step_name, step_definition in step.items() %}
        {%- set rule_number = step_definition["rules"] | length %}

        {%- set strict_join = '' %}
        {%- if rule_number == 2 and step_definition["type"].upper() == "STRICT" %}
            {%- set strict_join = 'INNER JOIN onboarding_rules_1 ST ON E.ACCOUNT_ID = ST.ACCOUNT_ID AND E.TIMESTAMP > ST.TIMESTAMP' %}

        {% endif %}
     
        {%- for rule in step_definition["rules"] %}
            {%- set rule_number = loop.index %}
            onboarding_rule_{{rule_number}} AS (
            {%- if rule["template"] == "count" %}
                
                WITH RankedEvents AS (
                    SELECT 
                        {%- if rule.get("group_by") == "user_id" %} MIN(E.TIMESTAMP) {%- else %} E.TIMESTAMP {%- endif %} AS TIMESTAMP,
                        E.ACCOUNT_ID,
                        E.USER_ID,
                        '{{ step_name }}' AS ONBOARDING_STEP,
                        ROW_NUMBER() OVER(PARTITION BY E.ACCOUNT_ID ORDER BY E.TIMESTAMP ASC) as RN
                    FROM EVENTS_AND_UX E
                    {{strict_join}}
                    {% if step_definition.get("time_limit") %}
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
                    ONBOARDING_STEP
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
                        '{{step_name}}' as ONBOARDING_STEP,
                        ROW_NUMBER() OVER (PARTITION BY E.ACCOUNT_ID ORDER BY MIN(E.TIMESTAMP)) as UserRank
                    FROM 
                        EVENTS_AND_UX E
                    {{ strict_join }}
                    {% if step_definition.get("time_limit") %}
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
                    ONBOARDING_STEP
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
                    {% if step_definition.get("time_limit") %}
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
                    '{{step_name}}' as ONBOARDING_STEP
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
            SELECT * FROM onboarding_rule_1 
        {%- elif rule_number > 1 %}
            ,
            combined_onboarding_rules AS
            (
            {%- for i in range(1, rule_number + 1) %}
                SELECT ACCOUNT_ID, USER_ID, TIMESTAMP, '{{ i }}' AS RULE_NUMBER
                FROM onboarding_rule_{{ i }}
                {%- if not loop.last %} UNION ALL {% endif %}
            {%- endfor %}
            )

            {% if step_definition.get('type', '').upper() == 'ANY' %}
                SELECT 
                    MIN(TIMESTAMP) as TIMESTAMP,
                    ACCOUNT_ID, 
                    USER_ID,
                    '{{step_name}}' as ONBOARDING_STEP
                FROM 
                    combined_onboarding_rules
                GROUP BY 
                    ACCOUNT_ID, USER_ID
            {% else %}
                SELECT 
                    MAX(TIMESTAMP) as TIMESTAMP,
                    ACCOUNT_ID, 
                    USER_ID,
                    '{{step_name}}' as ONBOARDING_STEP
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

all_onboarding_steps AS (
    {%- for i in range(1, onboarding_steps_count + 1) %}
        SELECT ACCOUNT_ID, USER_ID, TIMESTAMP, ONBOARDING_STEP
        FROM onboarding_step_{{ i }}
        {%- if not loop.last %} UNION ALL {% endif %}
    {%- endfor -%}
)

SELECT
    ACCOUNT_ID, 
    USER_ID, 
    TIMESTAMP, 
    ONBOARDING_STEP
FROM
    all_onboarding_steps
ORDER BY
    TIMESTAMP ASC