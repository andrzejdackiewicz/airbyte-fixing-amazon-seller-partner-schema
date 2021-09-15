{{ config(schema="TEST_NORMALIZATION", tags=["top-level"]) }}
-- Final base SQL model
select
    ID,
    DATE,
    PARTITION,
    _AIRBYTE_EMITTED_AT,
    _AIRBYTE_NESTED_STREAM_WITH_COMPLEX_COLUMNS_RESULTING_INTO_LONG_NAMES_HASHID
from {{ ref('NESTED_STREAM_WITH_COMPLEX_COLUMNS_RESULTING_INTO_LONG_NAMES_SCD') }}
-- NESTED_STREAM_WITH_COMPLEX_COLUMNS_RESULTING_INTO_LONG_NAMES from {{ source('TEST_NORMALIZATION', '_AIRBYTE_RAW_NESTED_STREAM_WITH_COMPLEX_COLUMNS_RESULTING_INTO_LONG_NAMES') }}
where _airbyte_active_row = 1

