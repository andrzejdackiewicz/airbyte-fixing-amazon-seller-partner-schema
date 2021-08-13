{{ config(schema="_AIRBYTE_TEST_NORMALIZATION", tags=["nested-intermediate"]) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    _AIRBYTE_CONFLICT_STREAM_NAME_2_HASHID,
    cast(GROUPS as {{ dbt_utils.type_string() }}) as GROUPS,
    _airbyte_emitted_at
from {{ ref('CONFLICT_STREAM_NAME_CONFLICT_STREAM_NAME_CONFLICT_STREAM_NAME_AB1') }}
-- CONFLICT_STREAM_NAME at conflict_stream_name/conflict_stream_name/conflict_stream_name

