{{ config(alias="exchange_rate", schema="test_normalization", tags=["top-level"]) }}
-- Final base SQL model
select
    id,
    currency,
    date,
    HKD,
    NZD,
    USD,
    _airbyte_emitted_at,
    _airbyte_exchange_rate_hashid
from {{ ref('_airbyte_test_normalization_exchange_rate_ab3') }}
-- exchange_rate from {{ source('test_normalization', '_airbyte_raw_exchange_rate') }}

