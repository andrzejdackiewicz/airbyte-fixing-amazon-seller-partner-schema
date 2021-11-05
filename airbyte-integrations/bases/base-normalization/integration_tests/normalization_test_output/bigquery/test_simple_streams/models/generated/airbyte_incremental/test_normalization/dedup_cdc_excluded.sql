{{ config(
    cluster_by = ["_airbyte_unique_key","_airbyte_emitted_at"],
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = "_airbyte_unique_key",
    schema = "test_normalization",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
select
    _airbyte_unique_key,
    id,
    name,
    _ab_cdc_lsn,
    _ab_cdc_updated_at,
    _ab_cdc_deleted_at,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_dedup_cdc_excluded_hashid
from {{ ref('dedup_cdc_excluded_scd') }}
-- dedup_cdc_excluded from {{ source('test_normalization', '_airbyte_raw_dedup_cdc_excluded') }}
where 1 = 1
and _airbyte_active_row = 1
{{ incremental_clause('_airbyte_emitted_at') }}

