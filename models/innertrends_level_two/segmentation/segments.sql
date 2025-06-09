{{ config(materialized="table", alias="ACCOUNTS_SEGMENTS") }}

{% set dates = get_date_range(var("client")) %}
{% set segments = get_segments_definition(var("client")) %}
{% set dataset = var('dataset', var('client')) %}
{%- set segments_tables = [] %}

-- depends_on: {{ ref('active_accounts') }}

{%- set segment_count = segments | length %}
{%- if segment_count > 0 %}
WITH
    {%- for segment_name, rulesets in segments.items() %}
        {%- set temp_table_name = segment_name | replace(" ", "_") | replace("-", "_") %}
        {%- set pair = {"segment": segment_name, "table": temp_table_name} %}
        {%- do segments_tables.append(pair) %}

        {{ temp_table_name | upper }} as (
            with
            {%- if rulesets["AND"] is not none %}
                {%- for ruleset in rulesets["AND"] %}
                        {%- set include_sql_list = [] %}
                    {%- for include_or_exclude, rule_group in ruleset.items() %}
                            {%- set rulegroup_table_name = (
                                temp_table_name ~ "_" ~ include_or_exclude
                            ) %}
                            {{ rulegroup_table_name | upper }} as (
                                with
                                {%- for andor, rules in rule_group.items() %}
                                        {%- set segment_rules_ctes = [] %}
                                        {%- for rule in rules %}
                                            {%- set rule_table_name = (
                                                temp_table_name
                                                ~ "_"
                                                ~ include_or_exclude
                                                ~ "_"
                                                ~ loop.index
                                            ) %}
                                            {%- set match_condition = retrieve_match(
                                                rule["match_type"],
                                                "account",
                                                rule["match_value"],
                                            ) %}
                                            {%- set value_timing = rule.get(
                                                "value_timing"
                                            ) %}
                                            {%- if value_timing %}
                                                {%- set time_filter_sql = retrieve_time_based_filter(
                                                    rule["match_property"],
                                                    value_timing,
                                                    match_condition,
                                                ) %}
                                            {%- else %}
                                                {%- set time_filter_sql = (
                                                    match_condition
                                                ) %}
                                            {%- endif %}
                                            {{ rule_table_name | upper }} as (
                                                {{ time_filter_sql }}
                                            )
                                            {%- if not loop.last %}, {% endif -%}
                                            {%- do segment_rules_ctes.append(
                                                rule_table_name | upper
                                            ) %}
                                        {%- endfor %}

                                    {% if segment_rules_ctes | length > 1 %}
                                        {%- if andor == "AND" %}
                                            {% for table in segment_rules_ctes %}
                                                select account_id
                                                from {{ table }}
                                                {%- if not loop.last %}
                                                    intersect distinct
                                                {%- endif %}
                                            {% endfor %}
                                        {%- elif andor == "OR" %}
                                            {% for table in segment_rules_ctes %}
                                                select account_id
                                                from {{ table }}
                                                {%- if not loop.last %}
                                                    union distinct
                                                {%- endif %}
                                            {% endfor %}
                                        {%- endif %}
                                    {%- else %}
                                        select distinct account_id
                                        from {{ segment_rules_ctes[0] }}
                                    {%- endif %}
                                {%- endfor %}
                            )
                            {%- if not loop.last %}, {% endif -%}

                        {%- if "EXCLUDE" in ruleset and "INCLUDE" not in ruleset %}
                            select distinct a.account_id
                            from {{ ref("active_accounts") }} a
                            where
                                not exists (
                                    select 1
                                    from {{ temp_table_name | upper }}_EXCLUDE b
                                    where a.account_id = b.account_id
                                )
                        {%- elif "EXCLUDE" in ruleset and include_or_exclude == "EXCLUDE" %}
                            select distinct a.account_id
                            from {{ temp_table_name | upper }}_INCLUDE a
                            where
                                not exists (
                                    select 1
                                    from {{ temp_table_name | upper }}_EXCLUDE b
                                    where a.account_id = b.account_id
                                )
                        {%- elif "EXCLUDE" not in ruleset %}
                            select distinct account_id
                            from {{ temp_table_name | upper }}_INCLUDE
                        {%- endif %}
                    {%- endfor %}
                {%- endfor %}
            {%- endif %}
        )
        {%- if not loop.last %}, {% endif -%}
    {%- endfor %}

{% for item in segments_tables %}
    select ACCOUNT_ID, "{{ item.segment }}" as SEGMENT
    from {{ item.table | upper }}
    {%- if not loop.last %}
        union all
    {% endif -%}
{% endfor %}


{% else %}

SELECT 
    NULL AS ACCOUNT_ID,
    NULL AS SEGMENT
FROM (SELECT 1) 
WHERE FALSE

{% endif %}