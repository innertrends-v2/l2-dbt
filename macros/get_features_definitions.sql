{% macro get_features_definition(client_var) %}
    {% set query %}
        SELECT features FROM DATA_SETTINGS.{{ client_var }}
    {% endset %}

    {% set results = run_query(query) %}

    {% if execute %}
        {% set features_json_string = results.columns[0].values()[0] %}

        -- Parse the JSON string into a Jinja data structure
        {% set features_definition = fromjson(features_json_string) %}

        {{ return(features_definition) }}
    {% else %}
        {{ return('{}') }}
    {% endif %}
{% endmacro %}
