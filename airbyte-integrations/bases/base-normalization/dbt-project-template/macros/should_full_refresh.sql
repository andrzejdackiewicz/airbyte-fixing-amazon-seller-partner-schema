{#
    This overrides the behavior of the macro `should_full_refresh` so full refresh are triggered if:
    - the dbt cli is run with --full-refresh flag or the model is configured explicitly to full_refresh
    - the column _airbyte_ab_id does not exists in the normalized tables and make sure it is well populated.
#}

{%- macro get_columns_in_relation_if_exist(target_table) -%}
    {{ return(adapter.dispatch('get_columns_in_relation_if_exist')(target_table)) }}
{%- endmacro -%}

{%- macro default__get_columns_in_relation_if_exist(target_table) -%}
    {{ return(adapter.get_columns_in_relation(target_table)) }}
{%- endmacro -%}

{%- macro databricks__get_columns_in_relation_if_exist(target_table) -%}
    {%- if target_table.schema is none -%}
        {%- set found_table = True %}
    {%- else -%}
    {% call statement('list_table_infos', fetch_result=True) -%}
        show tables in {{ target_table.schema }} like '*'
    {% endcall %}
    {%- set existing_tables = load_result('list_table_infos').table -%}
    {%- set found_table = [] %}
    {%- for table in existing_tables -%}
        {%- if table.tableName == target_table.identifier -%}
            {% do found_table.append(table.tableName) %}
        {%- endif -%}
    {%- endfor -%}
    {%- endif -%}
    {%- if found_table -%}
        {%- set cols = adapter.get_columns_in_relation(target_table) -%}
        {{ return(cols) }}
    {%- else -%}
        {{ return ([]) }}
    {%- endif -%}
{%- endmacro -%}

{%- macro need_full_refresh(col_ab_id, target_table=this) -%}
    {%- if not execute -%}
        {{ return(false) }}
    {%- endif -%}
    {%- set found_column = [] %}
    {%- set cols = get_columns_in_relation_if_exist(target_table) -%}
    {%- for col in cols -%}
        {%- if col.column == col_ab_id -%}
            {% do found_column.append(col.column) %}
        {%- endif -%}
    {%- endfor -%}
    {%- if found_column -%}
        {{ return(false) }}
    {%- else -%}
        {{ dbt_utils.log_info(target_table ~ "." ~ col_ab_id ~ " does not exist. The table needs to be rebuilt in full_refresh") }}
        {{ return(true) }}
    {%- endif -%}
{%- endmacro -%}

{%- macro should_full_refresh() -%}
  {% set config_full_refresh = config.get('full_refresh') %}
  {%- if config_full_refresh is none -%}
    {% set config_full_refresh = flags.FULL_REFRESH %}
  {%- endif -%}
  {%- if not config_full_refresh -%}
    {% set config_full_refresh = need_full_refresh(get_col_ab_id(), this) %}
  {%- endif -%}
  {% do return(config_full_refresh) %}
{%- endmacro -%}

{%- macro get_col_ab_id() -%}
  {{ adapter.dispatch('get_col_ab_id')() }}
{%- endmacro -%}

{%- macro default__get_col_ab_id() -%}
    _airbyte_ab_id
{%- endmacro -%}

{%- macro oracle__get_col_ab_id() -%}
    "_AIRBYTE_AB_ID"
{%- endmacro -%}

{%- macro snowflake__get_col_ab_id() -%}
    _AIRBYTE_AB_ID
{%- endmacro -%}
