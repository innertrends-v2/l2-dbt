{% macro get_onboarding_definition(client_var) %}
    {% set query %}
        SELECT ONBOARDING_STEPS FROM DATA_SETTINGS.{{ client_var }}
    {% endset %}

    {% set results = run_query(query) %}

    {% if execute %}
        {% set onboarding_steps_json_string = results.columns[0].values()[0] %}

        -- Parse the JSON string into a Jinja data structure
        {% set onboarding_steps = fromjson(onboarding_steps_json_string) %}

        {{ return(onboarding_steps) }}
    {% else %}
        {{ return('{}') }}
    {% endif %}
{% endmacro %}


{% macro get_activation_goals_definition(client_var) %}
    {% set query %}
        SELECT GOALS FROM DATA_SETTINGS.{{ client_var }}
    {% endset %}

    {% set results = run_query(query) %}

    {% if execute %}
        {% set activation_goals_json_string = results.columns[0].values()[0] %}

        -- Parse the JSON string into a Jinja data structure
        {% set activation_goals = fromjson(activation_goals_json_string) %}

        {{ return(activation_goals) }}
    {% else %}
        {{ return('{}') }}
    {% endif %}
{% endmacro %}

{% macro generate_activation_query(rule, prefix='') %}
                        ({% for condition in rule %}{% if not loop.first %} AND {% endif %}{{ retrieve_match(condition.match_type, condition.match_property, condition.match_value, prefix) }}{% endfor %})
{% endmacro %}