{{ config(schema="_AIRBYTE_TEST_NORMALIZATION", tags=["top-level-intermediate"]) }}
-- SQL model to build a hash column based on the values of this record
select
    {{ dbt_utils.surrogate_key([
        'ID',
        'CURRENCY',
        'DATE',
        'TIMESTAMP_COL',
        adapter.quote('HKD@spéçiäl & characters'),
        'HKD_SPECIAL___CHARACTERS',
        'NZD',
        'USD',
    ]) }} as _AIRBYTE_DEDUP_EXCHANGE_RATE_HASHID,
    tmp.*
from {{ ref('DEDUP_EXCHANGE_RATE_AB2') }} tmp
-- DEDUP_EXCHANGE_RATE

