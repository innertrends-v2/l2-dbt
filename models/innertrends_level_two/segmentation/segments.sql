{{ config(
    materialized = 'table',
    alias = 'ACCOUNTS_SEGMENTS'
) }}

{% set segment_definitions = get_segment_definitions(var('client')) %}


{% for segment_name, segment_ruleset in segment_definitions.items() %}
    
    -- retrieve the rulesets of the segment name

        -- for each ruleset

                -- if exclude in ruleset and include not in ruleset

                    -- generate CTE
    
    --call MACRO to CREATE CTE for each rule
{% endfor %}


{% if segment_definitions %}
    -- If segment_definitions exists and has content
    -- Select a simple output with one row for verification
    SELECT 
        '{{ var('client') }}' as client,
        {% for segment_name, segment_rules in segment_definitions.items() %}
            '{{ segment_name }}' as segment_{{ loop.index }}_name,
            '{{ segment_rules | tojson }}' as segment_{{ loop.index }}_condition{% if not loop.last %},{% endif %}
        {% endfor %}
{% else %}
    -- If segment_definitions doesn't exist or is empty
    SELECT 
        '{{ var('client') }}' as client,
        'No segment definitions found' as result
{% endif %}