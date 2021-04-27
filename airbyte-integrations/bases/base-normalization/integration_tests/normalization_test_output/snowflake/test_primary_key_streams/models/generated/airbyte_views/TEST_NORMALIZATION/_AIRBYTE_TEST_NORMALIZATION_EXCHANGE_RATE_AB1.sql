{{ config(alias="EXCHANGE_RATE_AB1", schema="_AIRBYTE_TEST_NORMALIZATION", tags=["top-level-intermediate"]) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    {{ json_extract_scalar('_airbyte_data', ['id']) }} as ID,
    {{ json_extract_scalar('_airbyte_data', ['currency']) }} as CURRENCY,
    {{ json_extract_scalar('_airbyte_data', ['date']) }} as DATE,
    {{ json_extract_scalar('_airbyte_data', ['HKD']) }} as HKD,
    {{ json_extract_scalar('_airbyte_data', ['NZD']) }} as NZD,
    {{ json_extract_scalar('_airbyte_data', ['USD']) }} as USD,
    _airbyte_emitted_at
from {{ source('TEST_NORMALIZATION', '_AIRBYTE_RAW_EXCHANGE_RATE') }}
-- EXCHANGE_RATE

