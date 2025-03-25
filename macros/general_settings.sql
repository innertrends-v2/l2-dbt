{% macro get_date_range(client_var) %}
    {% set query %}
        SELECT start_date 
        FROM DATA_SETTINGS.{{ client_var }}
    {% endset %}

    {% set results = run_query(query) %}

    {% if execute %}
        {% set start_date = results.columns[0].values()[0] %}
        {% set end_date = 'date_sub(current_date(), INTERVAL 1 DAY)' %}
        -- Returning both start_date and end_date as a dictionary
        {{ return({'start_date': start_date, 'end_date': end_date}) }}
    {% else %}
        {% set end_date = 'date_sub(current_date(), INTERVAL 1 DAY)' %}
        {{ return({'start_date': '2020-01-01', 'end_date': end_date}) }}  -- Default fallback
    {% endif %}
{% endmacro %}

{% macro retrieve_match(type, property, value) %}
    {% if property == "event" %}
        {% set property_match = "event" %}
    {% else %}
        {% set property_match = "json_extract_scalar(event_properties, '$." ~ property ~ "')" %}
    {% endif %}

    {% if type == "exact_match" %}
        ({{ property_match }} = '{{ value }}') 
    {% elif type == "contains" %}
        ({{ property_match }} LIKE '%{{ value }}%') 
    {% elif type == "regex" %}
        (REGEXP_CONTAINS({{ property_match }}, '{{ value }}')) 
    {% endif %}
{% endmacro %}

