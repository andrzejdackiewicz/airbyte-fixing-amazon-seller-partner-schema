

      create or replace transient table "AIRBYTE_DATABASE".TEST_NORMALIZATION_NAMESPACE."SIMPLE_STREAM_WITH_NAMESPACE_RESULTING_INTO_LONG_NAMES"  as
      (
-- Final base SQL model
select
    ID,
    DATE,
    _airbyte_emitted_at,
    _AIRBYTE_SIMPLE_STREAM_WITH_NAMESPACE_RESULTING_INTO_LONG_NAMES_HASHID
from "AIRBYTE_DATABASE"._AIRBYTE_TEST_NORMALIZATION_NAMESPACE."SIMPLE_STREAM_WITH_NAMESPACE_RESULTING_INTO_LONG_NAMES_AB3"
-- SIMPLE_STREAM_WITH_NAMESPACE_RESULTING_INTO_LONG_NAMES from "AIRBYTE_DATABASE".TEST_NORMALIZATION_NAMESPACE._AIRBYTE_RAW_SIMPLE_STREAM_WITH_NAMESPACE_RESULTING_INTO_LONG_NAMES
      );
    