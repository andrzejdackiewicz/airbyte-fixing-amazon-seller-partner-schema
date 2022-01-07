

      create or replace  table "AIRBYTE_DATABASE".TEST_NORMALIZATION."NESTED_STREAM_WITH_COMPLEX_COLUMNS_RESULTING_INTO_LONG_NAMES"  as
      (select * from(
            
-- Final base SQL model
-- depends_on: "AIRBYTE_DATABASE".TEST_NORMALIZATION."NESTED_STREAM_WITH_COMPLEX_COLUMNS_RESULTING_INTO_LONG_NAMES_SCD"
select
    _AIRBYTE_UNIQUE_KEY,
    ID,
    DATE,
    PARTITION,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_NESTED_STREAM_WITH_COMPLEX_COLUMNS_RESULTING_INTO_LONG_NAMES_HASHID
from "AIRBYTE_DATABASE".TEST_NORMALIZATION."NESTED_STREAM_WITH_COMPLEX_COLUMNS_RESULTING_INTO_LONG_NAMES_SCD"
-- NESTED_STREAM_WITH_COMPLEX_COLUMNS_RESULTING_INTO_LONG_NAMES from "AIRBYTE_DATABASE".TEST_NORMALIZATION._AIRBYTE_RAW_NESTED_STREAM_WITH_COMPLEX_COLUMNS_RESULTING_INTO_LONG_NAMES
where 1 = 1
and _AIRBYTE_ACTIVE_ROW = 1

            ) order by (_AIRBYTE_UNIQUE_KEY, _AIRBYTE_EMITTED_AT)
      );
    alter table "AIRBYTE_DATABASE".TEST_NORMALIZATION."NESTED_STREAM_WITH_COMPLEX_COLUMNS_RESULTING_INTO_LONG_NAMES" cluster by (_AIRBYTE_UNIQUE_KEY, _AIRBYTE_EMITTED_AT);