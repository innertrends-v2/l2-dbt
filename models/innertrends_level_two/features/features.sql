{{ config( 
    materialized = 'table',  
    alias = 'FEATURES'  
) }}

{% set dates = get_date_range(var('client')) %}
{% set features = get_features_definition(var('client')) %}
{%- set feature_ctes = [] %}

WITH 
    events_and_ux AS (
        /* Combine data from EVENTS and UX_INTERACTIONS tables */
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
    ),

{%- for feature_name, feature_def in features.items() if 'INCLUDE' in feature_def %}

    {{ feature_name }}_include AS (
        SELECT 
            timestamp, 
            event, 
            account_id, 
            user_id, 
            '{{ feature_name }}' AS feature
        FROM events_and_ux
        WHERE DATE(timestamp) BETWEEN '{{ dates.start_date }}' AND {{ dates.end_date }}
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
        {{ feature_name }}_exclude AS (
            SELECT 
                timestamp, 
                event, 
                account_id, 
                user_id, 
                feature
            FROM {{ feature_name }}_include
            WHERE (timestamp, event, account_id, user_id) NOT IN (
                SELECT 
                    timestamp, 
                    event, 
                    account_id, 
                    user_id
                FROM events_and_ux
                WHERE DATE(timestamp) BETWEEN '{{ dates.start_date }}' AND {{ dates.end_date }}
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
        {%- do feature_ctes.append(feature_name ~ '_exclude') %}
    {%- else %}
        {%- do feature_ctes.append(feature_name ~ '_include') %}
    {%- endif %}


    
{%- endfor %}

final_features AS (
    {%- for cte_name in feature_ctes %}
        SELECT TIMESTAMP, EVENT, ACCOUNT_ID, USER_ID, FEATURE FROM {{ cte_name }}
        {%- if not loop.last %} UNION ALL {% endif %}
    {%- endfor %}
)

-- Final SELECT to combine all feature data and return the result.
SELECT * FROM final_features
