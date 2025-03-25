{% macro get_features_definition(client_var) %}
    {% set query %}
        SELECT features FROM DATA_SETTINGS.{{ client_var }}
    {% endset %}

    {% set results = run_query(query) %}

    {% if execute %}
        {% set features_json_string = results.columns[0].values()[0] %}

        -- Parse the JSON string into a Jinja data structure
        {% set features_definition = fromjson(features_json_string) %}

        -- Create a dictionary for the cleaned feature names
        {% set cleaned_features = {} %}
        {% for feature_name, feature_value in features_definition.items() %}
            {% set clean_feature_name = feature_name | replace(" ", "_") | replace("_include", "") %}
            {% do cleaned_features.update({ clean_feature_name: feature_value }) %}
        {% endfor %}

        {{ return(cleaned_features) }}
    {% else %}
        {{ return('{}') }}
    {% endif %}
{% endmacro %}
