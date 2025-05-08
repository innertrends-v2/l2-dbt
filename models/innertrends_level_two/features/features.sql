{{ config( 
    materialized = 'table',  
    alias = 'FEATURES'  
) }}

{% set dates = get_date_range(var('client')) %}
{% set features = get_features_definition(var('client')) %}
{%- set feature_ctes = [] %}

{%- set features_count = features | length %}
{%- if features_count > 0 %}

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
    ),

{%- for feature_name, feature_def in features.items() if 'INCLUDE' in feature_def %}

    {%- set temp_table_name = feature_name | replace(" ", "_") | replace("-", "_") %}
    {{ temp_table_name | upper }}_INCLUDE AS (
        SELECT 
            TIMESTAMP, 
            EVENT, 
            ACCOUNT_ID, 
            USER_ID, 
            '{{ feature_name }}' AS FEATURE
        FROM EVENTS_AND_UX
        WHERE (
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
                a.TIMESTAMP, 
                a.EVENT, 
                a.ACCOUNT_ID, 
                a.USER_ID, 
                a.FEATURE
            FROM {{ feature_name }}_INCLUDE a
            LEFT JOIN (
                SELECT 
                    TIMESTAMP, 
                    EVENT, 
                    ACCOUNT_ID, 
                    USER_ID
                FROM EVENTS_AND_UX
                WHERE (
                    {%- for group in feature_def['EXCLUDE'] -%}
                        ({%- for rule in group -%}
                            {{- retrieve_match(rule['match_type'], rule['match_property'], rule['match_value']) -}}
                            {%- if not loop.last %} AND {% endif -%}
                        {%- endfor -%})
                        {%- if not loop.last %} OR {% endif -%}
                    {%- endfor -%}
                )
            ) b
            ON a.TIMESTAMP = b.TIMESTAMP
            AND a.EVENT = b.EVENT
            AND a.ACCOUNT_ID = b.ACCOUNT_ID
            -- AND a.USER_ID = b.USER_ID --userid being null can brake the query
            WHERE b.TIMESTAMP IS NULL
        )
    {%- endif %}
    ,
    {%- if 'EXCLUDE' in feature_def %}
        {%- do feature_ctes.append(temp_table_name | upper ~ '_EXCLUDE') %}
    {%- else %}
        {%- do feature_ctes.append(temp_table_name | upper ~ '_INCLUDE') %}
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

{% else %}

SELECT 
    NULL AS TIMESTAMP,
    NULL AS EVENT,
    NULL AS ACCOUNT_ID,
    NULL AS USER_ID,
    NULL AS FEATURE
FROM (SELECT 1) 
WHERE FALSE

{% endif %}