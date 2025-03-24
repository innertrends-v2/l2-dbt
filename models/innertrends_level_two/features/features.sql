{{ config(
    materialized = 'table',  
    alias = 'FEATURES'  
) }}

-- Macro to handle different match types (exact match, contains, or regex)
{% macro retrieve_match(type, property, value) %}
    {% if property == "event" %}
        {% set property_match = "event" %}  -- If the property is 'event', match it directly against the event column.
    {% else %}
        {% set property_match = "json_extract_scalar(event_properties, '$." ~ property ~ "')" %}  
        -- For any other property, extract the value from the event_properties JSON field.
    {% endif %}

    {% if type == "exact_match" %}
        ({{ property_match }} = '{{ value }}')  -- Exact match between the property and the provided value.
    {% elif type == "contains" %}
        ({{ property_match }} LIKE '%{{ value }}%')  -- Match if the property contains the given value.
    {% elif type == "regex" %}
        ({{ property_match }} RLIKE '{{ value }}')  -- Match if the property matches the provided regular expression.
    {% endif %}
{% endmacro %}

WITH settings AS (  -- Retrieve the start_date, end_date, and features_definition from the settings table.
    SELECT
        start_date,  -- The start date for the feature processing period.
        date_sub(current_date(), INTERVAL 1 DAY) AS end_date,  -- The end date is set to the day before the current date.
        features AS features_definition  -- The features definition, stored as JSON in the settings table.
    FROM
        DATA_SETTINGS.{{ var('client') }}  -- Retrieve settings for the specified client from the DATA_SETTINGS table.
),

events_and_ux AS (  -- Combine data from EVENTS and UX_INTERACTIONS tables.
    SELECT 
        timestamp, 
        event, 
        account_id, 
        user_id, 
        event_properties  -
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

features_union AS (  -- Define the processing logic for all features defined in the features_definition.

    {% set features = get_features_definition(var('client')) %}  -- Fetch the features definition for the client using the macro.

    {% for feature_name, feature_def in features.items() %}  -- Loop through each feature and its associated rules.
        {% if 'INCLUDE' in feature_def %}  -- Check if the feature has an 'INCLUDE' condition.

            {{ "-- " ~ feature_name ~ " logic" }}  -- Comment indicating the start of logic for the current feature.

            -- Process the 'INCLUDE' rules for the feature.
            WITH {{ feature_name }}_include AS (
                SELECT 
                    timestamp, 
                    event, 
                    account_id, 
                    user_id, 
                    '{{ feature_name }}' AS feature  -- Add the feature name to the results.
                FROM 
                    events_and_ux,
                    settings  -- Join with the settings CTE to get the start_date and end_date.
                WHERE 
                    DATE(timestamp) BETWEEN start_date AND end_date  -- Filter the events within the start and end dates.
                    AND (
                        {% for group in feature_def['INCLUDE'] %}  -- Loop through each group in INCLUDE.
                            (
                                {% for rule in group %}  -- Loop through each rule within the group.
                                    {{ retrieve_match(rule['match_type'], rule['match_property'], rule['match_value']) }}  -- Apply each match condition.
                                    {% if not loop.last %} AND {% endif %}  -- Conditions within the same group are joined by AND.
                                {% endfor %}
                            )
                            {% if not loop.last %} OR {% endif %}  -- Different groups of conditions are joined by OR.
                        {% endfor %}
                    )
            )

            {% if 'EXCLUDE' in feature_def %}  -- If the feature has an 'EXCLUDE' rule, apply it to filter out unwanted events.
                ,{{ feature_name }}_exclude AS (
                    SELECT 
                        timestamp, 
                        event, 
                        account_id, 
                        user_id, 
                        feature  -- Keep the feature name in the results.
                    FROM {{ feature_name }}_include  -- Start from the included events.
                    WHERE (timestamp, event, account_id, user_id) NOT IN (  -- Exclude events that match the 'EXCLUDE' rules.
                        SELECT 
                            timestamp, 
                            event, 
                            account_id, 
                            user_id 
                        FROM 
                            events_and_ux,
                            settings
                        WHERE 
                            DATE(timestamp) BETWEEN start_date AND end_date  -- Filter events based on the same start and end dates.
                            AND (
                                {% for group in feature_def['EXCLUDE'] %}  -- Loop through each group in EXCLUDE.
                                    (
                                        {% for rule in group %}  -- Loop through each rule within the group.
                                            {{ retrieve_match(rule['match_type'], rule['match_property'], rule['match_value']) }}  -- Apply each exclusion condition.
                                            {% if not loop.last %} AND {% endif %}  -- Conditions within the same group are joined by AND.
                                        {% endfor %}
                                    )
                                    {% if not loop.last %} OR {% endif %}  -- Different groups of exclusions are joined by OR.
                                {% endfor %}
                            )
                    )
                )
            {% endif %}

            -- Select the final result for the feature based on whether it has EXCLUDE conditions or not.
            SELECT * FROM {% if 'EXCLUDE' in feature_def %}{{ feature_name }}_exclude{% else %}{{ feature_name }}_include{% endif %}
            {% if not loop.last %} UNION ALL {% endif %}  -- Combine the results with other features.
        {% endif %}
    {% endfor %}
)

-- Final SELECT to combine all feature data and return the result.
SELECT * FROM features_union;
