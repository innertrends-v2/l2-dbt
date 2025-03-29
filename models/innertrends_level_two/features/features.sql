{{ config( 
    materialized = 'table',  
    alias = 'FEATURES'  
) }}

{% set dates = get_date_range(var('client')) %}
{% set features = get_features_definition(var('client')) %}
{%- set feature_ctes = [] %}

WITH 
    EVENTS_AND_UX AS (
        /* Combine data from EVENTS and UX_INTERACTIONS tables */
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
    ),

{%- for feature_name, feature_def in features.items() if 'INCLUDE' in feature_def %}

    {{ feature_name }}_INCLUDE AS (
        SELECT 
            TIMESTAMP, 
            EVENT, 
            ACCOUNT_ID, 
            USER_ID, 
            '{{ feature_name }}' AS FEATURE
        FROM EVENTS_AND_UX
        WHERE DATE(TIMESTAMP) BETWEEN '{{ dates.start_date }}' AND {{ dates.end_date }}
        AND (
            {%- for group in feature_def['INCLUDE'] -%}
                ({%- for rule in group -%}
                    {{- retrieve_match(rule['match_type'], rule['match_property'], rule['match_value']) -}}
                    {%- if not loop.last %} AND {% endif -%}
                {%- endfor -%})
                {%- if not loop.last %} OR {% endif -%}
            {%- endfor -%}
        )
    )

    {%- if 'EXCLUDE' in feature_def %}
        ,
        {{ feature_name }}_EXCLUDE AS (
            SELECT 
                TIMESTAMP, 
                EVENT, 
                ACCOUNT_ID, 
                USER_ID, 
                FEATURE
            FROM {{ feature_name }}_INCLUDE
            WHERE (TIMESTAMP, EVENT, ACCOUNT_ID, USER_ID) NOT IN (
                SELECT 
                    TIMESTAMP, 
                    EVENT, 
                    ACCOUNT_ID, 
                    USER_ID
                FROM EVENTS_AND_UX
                WHERE DATE(TIMESTAMP) BETWEEN '{{ dates.start_date }}' AND {{ dates.end_date }}
                AND (
                    {%- for group in feature_def['EXCLUDE'] -%}
                        ({%- for rule in group -%}
                            {{- retrieve_match(rule['match_type'], rule['match_property'], rule['match_value']) -}}
                            {%- if not loop.last %} AND {% endif -%}
                        {%- endfor -%})
                        {%- if not loop.last %} OR {% endif -%}
                    {%- endfor -%}
                )
            )
        )
    {%- endif %}
    ,
    {%- if 'EXCLUDE' in feature_def %}
        {%- do feature_ctes.append(feature_name ~ '_EXCLUDE') %}
    {%- else %}
        {%- do feature_ctes.append(feature_name ~ '_INCLUDE') %}
    {%- endif %}

    
{%- endfor %}

FINAL_FEATURES AS (
    {%- for cte_name in feature_ctes %}
        SELECT TIMESTAMP, EVENT, ACCOUNT_ID, USER_ID, FEATURE FROM {{ cte_name }}
        {%- if not loop.last %} UNION ALL {% endif %}
    {%- endfor %}
)

-- Final SELECT to combine all feature data and return the result.
SELECT TIMESTAMP, EVENT, ACCOUNT_ID, USER_ID, FEATURE FROM FINAL_FEATURES