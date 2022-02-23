{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_test_normalization",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    _airbyte_nested_strea__nto_long_names_hashid,
    {{ json_extract_array(adapter.quote('partition'), ['double_array_data'], ['double_array_data']) }} as double_array_data,
    {{ json_extract_array(adapter.quote('partition'), ['DATA'], ['DATA']) }} as {{ adapter.quote('DATA') }},
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('nested_stream_with_co_1g_into_long_names_scd') }} as table_alias
-- partition at nested_stream_with_complex_columns_resulting_into_long_names/partition
where 1 = 1
and {{ adapter.quote('partition') }} is not null
{{ incremental_clause('_airbyte_emitted_at') }}

