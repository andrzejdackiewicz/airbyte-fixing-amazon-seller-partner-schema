
      

  create  table "postgres".test_normalization."renamed_dedup_cdc_excluded"
  as (
    
-- Final base SQL model
-- depends_on: "postgres".test_normalization."renamed_dedup_cdc_excluded_scd"
select
    _airbyte_unique_key,
    "id",
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at,
    _airbyte_renamed_dedup_cdc_excluded_hashid
from "postgres".test_normalization."renamed_dedup_cdc_excluded_scd"
-- renamed_dedup_cdc_excluded from "postgres".test_normalization._airbyte_raw_renamed_dedup_cdc_excluded
where 1 = 1
and _airbyte_active_row = 1

  );
  