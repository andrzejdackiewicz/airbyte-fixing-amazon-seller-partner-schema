{{ config(schema="_airbyte_test_normalization", tags=["nested-intermediate"]) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
{{ unnest_cte('nested_stream_with_c___long_names_partition', 'partition', adapter.quote('DATA')) }}
select
    _airbyte_partition_hashid,
    {{ json_extract_scalar(unnested_column_value(adapter.quote('DATA')), ['currency']) }} as currency,
    _airbyte_emitted_at
from {{ ref('nested_stream_with_c___long_names_partition') }}
{{ cross_join_unnest('partition', adapter.quote('DATA')) }}
where {{ adapter.quote('DATA') }} is not null
-- DATA at nested_stream_with_complex_columns_resulting_into_long_names/partition/DATA

