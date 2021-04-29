{{ config(alias="dedup_exchange_rate_ab3", schema="_airbyte_test_normalization", tags=["top-level-intermediate"]) }}
-- SQL model to build a hash column based on the values of this record
select
    *,
    {{ dbt_utils.surrogate_key([
        adapter.quote('id'),
        'currency',
        adapter.quote('date'),
        adapter.quote('HKD@spéçiäl & characters'),
        'nzd',
        'usd',
    ]) }} as _airbyte_dedup_exchange_rate_hashid
from {{ ref('dedup_exchange_rate_ab2_281') }}
-- dedup_exchange_rate

