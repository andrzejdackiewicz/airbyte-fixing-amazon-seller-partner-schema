{{ config(alias="nested_stream_with_complex_col_669_data_ab3", schema="_airbyte_test_normalization", tags=["nested-intermediate"]) }}
-- SQL model to build a hash column based on the values of this record
select
    *,
    {{ dbt_utils.surrogate_key([
        '_airbyte_partition_hashid',
        'currency',
    ]) }} as _airbyte_data_hashid
from {{ ref('nested_stream_with_complex_col_669_data_ab2_ff0') }}
-- DATA at nested_stream_with_complex_columns_resulting_into_long_names/partition/DATA

