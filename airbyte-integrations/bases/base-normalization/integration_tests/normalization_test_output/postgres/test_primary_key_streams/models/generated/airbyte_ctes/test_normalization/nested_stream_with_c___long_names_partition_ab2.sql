{{ config(schema="_airbyte_test_normalization", tags=["nested-intermediate"]) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    _airbyte_nested_stre__nto_long_names_hashid,
    double_array_data,
    {{ adapter.quote('DATA') }},
    _airbyte_emitted_at
from {{ ref('nested_stream_with_c___long_names_partition_ab1') }}
-- partition at nested_stream_with_complex_columns_resulting_into_long_names/partition

