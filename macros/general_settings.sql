{% macro get_date_range(client_var) %}
    {% set query %}
        SELECT start_date FROM innertrends-clients.DATA_SETTINGS.{{ client_var }} ORDER BY SETTINGS_TIMESTAMP DESC LIMIT 1
    {% endset %}

    {% set results = run_query(query) %}

    {% if execute %}
        {% set start_date = results.columns[0].values()[0] %}
        {% set end_date = 'current_date()' %}
        -- Returning both start_date and end_date as a dictionary
        {{ return({'start_date': start_date, 'end_date': end_date}) }}
    {% else %}
        {% set end_date = 'current_date()' %}
        {{ return({'start_date': '2020-01-01', 'end_date': end_date}) }}  -- Default fallback
    {% endif %}
{% endmacro %}

{% macro retrieve_match(type, property, value, prefix='') %}
    {%- set prefix = prefix if prefix else '' -%}

    {%- if property == "event" -%}
        {%- set property_match = prefix ~ "EVENT" -%}
    {%- elif property == 'account' -%}
        {%- set property_match = prefix ~ "PROPERTY_VALUE" -%}
    {%- else -%}
        {%- set property_match = "JSON_EXTRACT_SCALAR(" ~ prefix ~ "EVENT_PROPERTIES, '$." ~ property ~ "')" -%}
    {%- endif -%}

    {%- if type == "exact_match" -%}
        ({{ property_match }} = '{{ value }}')
    {%- elif type == "contains" -%}
        ({{ property_match }} LIKE '%{{ value }}%')
    {%- elif type == "regex" -%}
        (REGEXP_CONTAINS({{ property_match }}, '{{ value }}'))
    {%- endif -%}
{% endmacro %}


{% macro schema_stub(column_defs) %}
SELECT
{%- for col in column_defs %}
    CAST(NULL AS {{ col.type }}) AS {{ col.name }}{% if not loop.last %},{% endif %}
{%- endfor %}
FROM (SELECT 1)
WHERE FALSE
{% endmacro %}