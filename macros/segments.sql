{% macro get_segment_definitions(client_var) %}
    {% set query %}
        SELECT accounts_segments FROM DATA_SETTINGS.{{ client_var }}
    {% endset %}
    
    {% set results = run_query(query) %}
    
    {% if execute %}
        {% set segment_json_string = results.columns[0].values()[0] %}
        
        -- Parse the JSON string into a Jinja data structure
        {% set segment_definitions = fromjson(segment_json_string) %}
        
        {{ return(segment_definitions) }}
    {% else %}
        {{ return('{}') }}
    {% endif %}
{% endmacro %}