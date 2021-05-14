{{ config(alias="nested_stream_with_c__g_into_long_names_ab4", schema="_airbyte_test_normalization", tags=["top-level-intermediate"]) }}
-- SQL model to prepare for deduplicating records based on the hash record column
select
  *,
  row_number() over (
    partition by _airbyte_nested_stre__nto_long_names_hashid
    order by _airbyte_emitted_at asc
  ) as _airbyte_row_num
from {{ ref('nested_stream_with_c__g_into_long_names_ab3_301') }}
-- nested_stream_with_c__lting_into_long_names from {{ source('test_normalization', '_airbyte_raw_nested_stream_with_complex_columns_resulting_into_long_names') }}

