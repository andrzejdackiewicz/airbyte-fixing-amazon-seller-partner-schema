{{ config(schema="_airbyte_test_normalization", tags=["nested-intermediate"]) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    _airbyte_conflict_stream_array_2_hashid,
    cast({{ adapter.quote('id') }} as {{ dbt_utils.type_bigint() }}) as {{ adapter.quote('id') }},
    _airbyte_emitted_at
from {{ ref('conflict_stream_arra___conflict_stream_name_ab1') }}
-- conflict_stream_name at conflict_stream_array/conflict_stream_array/conflict_stream_name

