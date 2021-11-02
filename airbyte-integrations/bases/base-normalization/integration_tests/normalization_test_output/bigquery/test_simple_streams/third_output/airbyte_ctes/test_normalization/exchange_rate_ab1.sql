

  create or replace view `dataline-integration-testing`._airbyte_test_normalization.`exchange_rate_ab1`
  OPTIONS()
  as 
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    json_extract_scalar(_airbyte_data, "$['id']") as id,
    json_extract_scalar(_airbyte_data, "$['currency']") as currency,
    json_extract_scalar(_airbyte_data, "$['new_column']") as new_column,
    json_extract_scalar(_airbyte_data, "$['date']") as date,
    json_extract_scalar(_airbyte_data, "$['timestamp_col']") as timestamp_col,
    json_extract_scalar(_airbyte_data, "$['HKD@spéçiäl & characters']") as HKD_special___characters,
    json_extract_scalar(_airbyte_data, "$['NZD']") as NZD,
    json_extract_scalar(_airbyte_data, "$['USD']") as USD,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from `dataline-integration-testing`.test_normalization._airbyte_raw_exchange_rate as table_alias
-- exchange_rate
where 1 = 1
;

