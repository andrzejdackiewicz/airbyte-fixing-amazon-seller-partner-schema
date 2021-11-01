

      create or replace transient table "AIRBYTE_DATABASE".TEST_NORMALIZATION."NON_NESTED_STREAM_WITHOUT_NAMESPACE_RESULTING_INTO_LONG_NAMES"  as
      (select * from(
            
-- Final base SQL model
select
    ID,
    DATE,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_NON_NESTED_STREAM_WITHOUT_NAMESPACE_RESULTING_INTO_LONG_NAMES_HASHID
from "AIRBYTE_DATABASE"._AIRBYTE_TEST_NORMALIZATION."NON_NESTED_STREAM_WITHOUT_NAMESPACE_RESULTING_INTO_LONG_NAMES_AB3"
-- NON_NESTED_STREAM_WITHOUT_NAMESPACE_RESULTING_INTO_LONG_NAMES from "AIRBYTE_DATABASE".TEST_NORMALIZATION._AIRBYTE_RAW_NON_NESTED_STREAM_WITHOUT_NAMESPACE_RESULTING_INTO_LONG_NAMES
where 1 = 1
            ) order by (_AIRBYTE_EMITTED_AT)
      );
    alter table "AIRBYTE_DATABASE".TEST_NORMALIZATION."NON_NESTED_STREAM_WITHOUT_NAMESPACE_RESULTING_INTO_LONG_NAMES" cluster by (_AIRBYTE_EMITTED_AT);