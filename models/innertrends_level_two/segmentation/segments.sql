{{ config( 
    materialized = 'table',  
    alias = 'ACCOUNTS_SEGMENTS'  
) }}

{% set dates = get_date_range(var('client')) %}
{% set segments = get_segments_definition(var('client')) %}
{%- set segments_tables = [] %}

-- depends_on: {{ ref('active_accounts') }}

WITH 
{%- for segment_name, rulesets in segments.items() %}
    {%- set temp_table_name = segment_name | replace(" ", "_") %}
    {%- set pair = {"segment": segment_name, "table": temp_table_name} %}
    {%- do segments_tables.append(pair) %}
    
    {{temp_table_name | upper}} AS (
        WITH
    {%- if rulesets["AND"] is not none %}
        {%- for ruleset in rulesets["AND"] %}
            {%- set include_sql_list = [] %}
            {%- for include_or_exclude, rule_group in ruleset.items() %}
                {%- set rulegroup_table_name = temp_table_name ~ "_"~ include_or_exclude %}
                {{rulegroup_table_name | upper}} AS (
                    WITH
                    {%- for andor, rules in rule_group.items() %}
                        {%- set segment_rules_ctes = [] %}
                        {%- for rule in rules %}
                            {%- set rule_table_name = temp_table_name ~ "_"~ include_or_exclude ~"_" ~ loop.index %}
                            {%- set match_condition = retrieve_match(rule["match_type"], 'account', rule["match_value"]) %}
                            {%- set value_timing = rule.get("value_timing") %}
                            {%- if value_timing %}
                                {%- set time_filter_sql = retrieve_time_based_filter(rule["match_property"], value_timing, match_condition) %}
                            {%- else %}
                                {%- set time_filter_sql = match_condition %}
                            {%- endif %}
                            {{rule_table_name | upper}} AS (
                                {{time_filter_sql}}
                            ){%- if not loop.last %} , {% endif -%}
                            {%- do segment_rules_ctes.append(rule_table_name | upper) %}
                        {%- endfor %}

                        {% if segment_rules_ctes | length > 1 %}
                            {%- if andor == "AND" %}
                                {% for table in segment_rules_ctes %}
                                    SELECT ACCOUNT_ID FROM {{ table }}
                                    {%- if not loop.last %}
                                        INTERSECT DISTINCT
                                    {%- endif %}
                                {% endfor %}                      
                            {%- elif andor == "OR" %}
                                {% for table in segment_rules_ctes %}
                                    SELECT ACCOUNT_ID FROM {{ table }}
                                    {%- if not loop.last %}
                                        UNION DISTINCT
                                    {%- endif %}
                                {% endfor %} 
                            {%- endif %}
                        {%- else %}
                            SELECT DISTINCT ACCOUNT_ID FROM {{segment_rules_ctes[0]}}
                        {%- endif %}
                    {%- endfor %}
                ){%- if not loop.last %} , {% endif -%}

                {%- if "EXCLUDE" in ruleset and "INCLUDE" not in ruleset %}
                    SELECT DISTINCT a.ACCOUNT_ID 
                    FROM {{ ref('active_accounts') }} a 
                    WHERE NOT EXISTS (
                        SELECT 1 
                        FROM {{temp_table_name | upper}}_EXCLUDE b 
                        WHERE a.ACCOUNT_ID = b.ACCOUNT_ID
                    )
                {%- elif "EXCLUDE" in ruleset %}
                    SELECT DISTINCT a.ACCOUNT_ID 
                    FROM {{temp_table_name | upper}}_INCLUDE a 
                    WHERE NOT EXISTS (
                        SELECT 1 
                        FROM {{temp_table_name | upper}}_EXCLUDE b 
                        WHERE a.ACCOUNT_ID = b.ACCOUNT_ID
                    )
                {%- elif "INCLUDE" in ruleset %}
                    SELECT DISTINCT ACCOUNT_ID 
                    FROM {{temp_table_name | upper}}_INCLUDE
                {%- endif %}
            {%- endfor %}
        {%- endfor %}
    {%- endif %}
    ){%- if not loop.last %} , {% endif -%}
{%- endfor %}

{% for item in segments_tables %}
SELECT 
    ACCOUNT_ID,
    "{{ item.segment }}" AS SEGMENT_NAME
FROM
    {{ item.table | upper }}
{%- if not loop.last %} UNION ALL {% endif -%}
{% endfor %}