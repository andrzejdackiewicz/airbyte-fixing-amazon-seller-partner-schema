

  create or replace view `dataline-integration-testing`._airbyte_test_normalization.`renamed_dedup_cdc_excluded_ab1`
  OPTIONS()
  as 
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    json_extract_scalar(_airbyte_data, "$['id']") as id,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from `dataline-integration-testing`.test_normalization._airbyte_raw_renamed_dedup_cdc_excluded as table_alias
-- renamed_dedup_cdc_excluded
where 1 = 1
;

