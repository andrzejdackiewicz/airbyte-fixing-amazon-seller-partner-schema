
      delete
    from "test_normalization".test_normalization."renamed_dedup_cdc_excluded"
    where (_airbyte_unique_key) in (
        select (_airbyte_unique_key)
        from "test_normalization".test_normalization."#renamed_dedup_cdc_excluded__dbt_tmp"
    );

    insert into "test_normalization".test_normalization."renamed_dedup_cdc_excluded" ("_airbyte_unique_key", "id", "_airbyte_ab_id", "_airbyte_emitted_at", "_airbyte_normalized_at", "_airbyte_renamed_dedup_cdc_excluded_hashid")
    (
       select "_airbyte_unique_key", "id", "_airbyte_ab_id", "_airbyte_emitted_at", "_airbyte_normalized_at", "_airbyte_renamed_dedup_cdc_excluded_hashid"
       from "test_normalization".test_normalization."#renamed_dedup_cdc_excluded__dbt_tmp"
    );
  