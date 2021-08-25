{{ config(schema="_airbyte_test_normalization", tags=["top-level-intermediate"]) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as id,
    {{ json_extract_array('_airbyte_data', ['children'], ['children']) }} as children,
    _airbyte_emitted_at
from {{ source('test_normalization', '_airbyte_raw_unnest_alias') }} as table_alias
-- unnest_alias

