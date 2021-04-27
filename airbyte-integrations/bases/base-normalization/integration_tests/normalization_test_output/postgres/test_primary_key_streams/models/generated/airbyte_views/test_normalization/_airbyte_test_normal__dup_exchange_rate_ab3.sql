{{ config(alias="dedup_exchange_rate_ab3", schema="_airbyte_test_normalization", tags=["top-level-intermediate"]) }}
-- SQL model to build a hash column based on the values of this record
select
    *,
    {{ dbt_utils.surrogate_key([
        adapter.quote('id'),
        'currency',
        adapter.quote('date'),
        'hkd',
        'nzd',
        'usd',
    ]) }} as _airbyte_dedup_exchange_rate_hashid
from {{ ref('_airbyte_test_normal__dup_exchange_rate_ab2') }}
-- dedup_exchange_rate

