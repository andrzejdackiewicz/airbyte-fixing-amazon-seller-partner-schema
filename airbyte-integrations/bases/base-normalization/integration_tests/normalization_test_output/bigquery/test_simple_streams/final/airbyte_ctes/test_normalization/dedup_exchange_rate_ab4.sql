

  create or replace view `dataline-integration-testing`._airbyte_test_normalization.`dedup_exchange_rate_ab4`
  OPTIONS()
  as 
-- SQL model to prepare for deduplicating records based on the hash record column
select
  row_number() over (
    partition by _airbyte_dedup_exchange_rate_hashid
    order by _airbyte_emitted_at asc
  ) as _airbyte_row_num,
  tmp.*
from `dataline-integration-testing`._airbyte_test_normalization.`dedup_exchange_rate_ab3` tmp
-- dedup_exchange_rate from `dataline-integration-testing`.test_normalization._airbyte_raw_dedup_exchange_rate;

