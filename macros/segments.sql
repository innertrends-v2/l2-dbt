{% macro get_segments_definition(client_var) %}
    {% set query %}
        SELECT ACCOUNTS_SEGMENTS FROM DATA_SETTINGS.{{ client_var }} ORDER BY SETTINGS_TIMESTAMP DESC LIMIT 1
    {% endset %}

    {% set results = run_query(query) %}

    {% if execute %}
        {% set segments_json_string = results.columns[0].values()[0] %}

        -- Parse the JSON string into a Jinja data structure
        {% set segments_definition = fromjson(segments_json_string) %}

        -- Create a dictionary for the cleaned segment names, excluding empty objects
        {% set cleaned_segments = {} %}
        {% for segment_name, segment_value in segments_definition.items() %}
            {% if segment_value != {} %}
                {% do cleaned_segments.update({ segment_name: segment_value }) %}
            {% endif %}
        {% endfor %}

        {{ return(cleaned_segments) }}
    {% else %}
        {{ return('{}') }}
    {% endif %}
{% endmacro %}


{% macro retrieve_time_based_filter(property, value_timing, sql_condition) %}

    {% if value_timing == "first" %}
        {% set filter_sql %}
                                    SELECT 
                                        ACCOUNT_ID, 
                                        TIMESTAMP, 
                                        PROPERTY_VALUE
                                    FROM 
                                    (        
                                        WITH ranked_properties AS (
                                            SELECT 
                                                ACCOUNT_ID, 
                                                TIMESTAMP, 
                                                PROPERTY_VALUE,
                                                ROW_NUMBER() OVER (PARTITION BY ACCOUNT_ID ORDER BY TIMESTAMP ASC) AS row_num
                                            FROM {{ var('client') }}.ACCOUNTS_PROPERTIES
                                            WHERE PROPERTY_KEY = '{{ property }}'
                                        )
                                        SELECT 
                                            ACCOUNT_ID, 
                                            TIMESTAMP, 
                                            PROPERTY_VALUE
                                        FROM ranked_properties
                                        WHERE row_num = 1 AND {{ sql_condition }}
                                    )
        {% endset %}

    {% elif value_timing == "last" %}
        {% set filter_sql %}
                                    SELECT 
                                        ACCOUNT_ID, 
                                        TIMESTAMP, 
                                        PROPERTY_VALUE
                                    FROM 
                                    (   
                                        WITH ranked_properties AS (
                                            SELECT 
                                                ACCOUNT_ID, 
                                                TIMESTAMP, 
                                                PROPERTY_VALUE,
                                                ROW_NUMBER() OVER (PARTITION BY ACCOUNT_ID ORDER BY TIMESTAMP DESC) AS row_num
                                            FROM {{ var('client') }}.ACCOUNTS_PROPERTIES
                                            WHERE PROPERTY_KEY = '{{ property }}'
                                        )
                                        SELECT 
                                            ACCOUNT_ID, 
                                            TIMESTAMP, 
                                            PROPERTY_VALUE
                                        FROM ranked_properties
                                        WHERE row_num = 1 AND {{ sql_condition }}
                                    )
        {% endset %}

    {% elif value_timing == "any" %}
        {% set filter_sql %}
        SELECT 
            ACCOUNT_ID, 
            TIMESTAMP, 
            PROPERTY_VALUE
        FROM {{ var('client') }}.ACCOUNTS_PROPERTIES
        WHERE PROPERTY_KEY = '{{ property }}' AND {{ sql_condition }}
        {% endset %}

    {% else %}
        {% set filter_sql %}
        {{ exceptions.raise_compiler_error("Invalid value_timing: " ~ value_timing) }}
        {% endset %}
    {% endif %}

    {{ return(filter_sql) }}

{% endmacro %}

