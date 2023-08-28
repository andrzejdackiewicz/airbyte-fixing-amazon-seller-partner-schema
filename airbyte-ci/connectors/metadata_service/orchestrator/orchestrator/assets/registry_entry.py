#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import copy
import json
import os
from typing import List, Optional, Tuple, Union

import orchestrator.hacks as HACKS
import pandas as pd
import sentry_sdk
import yaml
from dagster import AutoMaterializePolicy, DynamicPartitionsDefinition, MetadataValue, OpExecutionContext, Output, asset
from dagster_gcp.gcs.file_manager import GCSFileHandle, GCSFileManager
from google.cloud import storage
from metadata_service.constants import ICON_FILE_NAME, METADATA_FILE_NAME
from metadata_service.models.generated.ConnectorRegistryDestinationDefinition import ConnectorRegistryDestinationDefinition
from metadata_service.models.generated.ConnectorRegistrySourceDefinition import ConnectorRegistrySourceDefinition
from metadata_service.spec_cache import get_cached_spec, list_cached_specs
from orchestrator.config import MAX_METADATA_PARTITION_RUN_REQUEST, VALID_REGISTRIES, get_public_url_for_gcs_file
from orchestrator.logging import sentry
from orchestrator.logging.publish_connector_lifecycle import PublishConnectorLifecycle, PublishConnectorLifecycleStage, StageStatus
from orchestrator.models.metadata import LatestMetadataEntry, MetadataDefinition
from orchestrator.utils.dagster_helpers import OutputDataFrame
from orchestrator.utils.object_helpers import deep_copy_params
from pydantic import ValidationError
from pydash.objects import get

PolymorphicRegistryEntry = Union[ConnectorRegistrySourceDefinition, ConnectorRegistryDestinationDefinition]
TaggedRegistryEntry = Tuple[str, PolymorphicRegistryEntry]

GROUP_NAME = "registry_entry"

metadata_partitions_def = DynamicPartitionsDefinition(name="metadata")

# ERRORS


class MissingCachedSpecError(Exception):
    pass


# HELPERS


@sentry_sdk.trace
def apply_spec_to_registry_entry(registry_entry: dict, cached_specs: OutputDataFrame) -> dict:
    cached_connector_version = {
        (cached_spec["docker_repository"], cached_spec["docker_image_tag"]): cached_spec["spec_cache_path"]
        for cached_spec in cached_specs.to_dict(orient="records")
    }

    try:
        spec_path = cached_connector_version[(registry_entry["dockerRepository"], registry_entry["dockerImageTag"])]
        entry_with_spec = copy.deepcopy(registry_entry)
        entry_with_spec["spec"] = get_cached_spec(spec_path)
        return entry_with_spec
    except KeyError:
        raise MissingCachedSpecError(f"No cached spec found for {registry_entry['dockerRepository']}:{registry_entry['dockerImageTag']}")


def calculate_migration_documentation_url(releases_or_breaking_change: dict, documentation_url: str, version: Optional[str] = None) -> str:
    """Calculate the migration documentation url for the connector releases.

    Args:
        metadata_releases (dict): The connector releases.

    Returns:
        str: The migration documentation url.
    """

    base_url = f"{documentation_url}-migrations"
    default_migration_documentation_url = f"{base_url}#{version}" if version is not None else base_url

    return releases_or_breaking_change.get("migrationDocumentationUrl", None) or default_migration_documentation_url


@deep_copy_params
def apply_connector_release_defaults(metadata: dict) -> Optional[pd.DataFrame]:
    metadata_releases = metadata.get("releases")
    documentation_url = metadata.get("documentationUrl")
    if metadata_releases is None:
        return None

    # apply defaults for connector releases
    metadata_releases["migrationDocumentationUrl"] = calculate_migration_documentation_url(metadata_releases, documentation_url)

    # releases has a dictionary field called breakingChanges, where the key is the version and the value is the data for the breaking change
    # each breaking change has a migrationDocumentationUrl field that is optional, so we need to apply defaults to it
    breaking_changes = metadata_releases["breakingChanges"]
    if breaking_changes is not None:
        for version, breaking_change in breaking_changes.items():
            breaking_change["migrationDocumentationUrl"] = calculate_migration_documentation_url(
                breaking_change, documentation_url, version
            )

    return metadata_releases


@deep_copy_params
def apply_overrides_from_registry(metadata_data: dict, override_registry_key: str) -> dict:
    """Apply the overrides from the registry to the metadata data.

    Args:
        metadata_data (dict): The metadata data field.
        override_registry_key (str): The key of the registry to override the metadata with.

    Returns:
        dict: The metadata data field with the overrides applied.
    """
    override_registry = metadata_data["registries"][override_registry_key]
    del override_registry["enabled"]

    # remove any None values from the override registry
    override_registry = {k: v for k, v in override_registry.items() if v is not None}

    metadata_data.update(override_registry)

    return metadata_data


@deep_copy_params
def apply_ab_internal_defaults(metadata_data: dict) -> dict:
    """Apply ab_internal defaults to the metadata data field.

    Args:
        metadata_data (dict): The metadata data field.

    Returns:
        dict: The metadata data field with the ab_internal defaults applied.
    """
    default_ab_internal_values = {
        "sl": 100,
        "ql": 100,
    }

    existing_ab_internal_values = metadata_data.get("ab_internal") or {}
    ab_internal_values = {**default_ab_internal_values, **existing_ab_internal_values}

    metadata_data["ab_internal"] = ab_internal_values

    return metadata_data


@deep_copy_params
@sentry_sdk.trace
def metadata_to_registry_entry(metadata_entry: LatestMetadataEntry, override_registry_key: str) -> dict:
    """Convert the metadata definition to a registry entry.

    Args:
        metadata_definition (dict): The metadata definition.
        connector_type (str): One of "source" or "destination".
        override_registry_key (str): The key of the registry to override the metadata with.

    Returns:
        dict: The registry equivalent of the metadata definition.
    """
    metadata_definition = metadata_entry.metadata_definition.dict()
    metadata_data = metadata_definition["data"]
    connector_type = metadata_data["connectorType"]

    # apply overrides from the registry
    overridden_metadata_data = apply_overrides_from_registry(metadata_data, override_registry_key)

    # remove fields that are not needed in the registry
    del overridden_metadata_data["registries"]
    del overridden_metadata_data["connectorType"]

    # rename field connectorSubtype to sourceType
    connector_subtype = overridden_metadata_data.get("connectorSubtype")
    if connector_subtype:
        overridden_metadata_data["sourceType"] = overridden_metadata_data["connectorSubtype"]
        del overridden_metadata_data["connectorSubtype"]

    # rename definitionId field to sourceDefinitionId or destinationDefinitionId
    id_field = "sourceDefinitionId" if connector_type == "source" else "destinationDefinitionId"
    overridden_metadata_data[id_field] = overridden_metadata_data["definitionId"]
    del overridden_metadata_data["definitionId"]

    # add in useless fields that are currently required for porting to the actor definition spec
    overridden_metadata_data["tombstone"] = False
    overridden_metadata_data["custom"] = False
    overridden_metadata_data["public"] = True

    # if there is no supportLevel, set it to "community"
    if not overridden_metadata_data.get("supportLevel"):
        overridden_metadata_data["supportLevel"] = "community"

    # apply ab_internal defaults
    overridden_metadata_data = apply_ab_internal_defaults(overridden_metadata_data)

    # apply generated fields
    overridden_metadata_data["iconUrl"] = metadata_entry.icon_url
    overridden_metadata_data["releases"] = apply_connector_release_defaults(overridden_metadata_data)

    return overridden_metadata_data


@sentry_sdk.trace
def read_registry_entry_blob(registry_entry_blob: storage.Blob) -> TaggedRegistryEntry:
    json_string = registry_entry_blob.download_as_string().decode("utf-8")
    registry_entry_dict = json.loads(json_string)

    connector_type, ConnectorModel = get_connector_type_from_registry_entry(registry_entry_dict)
    registry_entry = ConnectorModel.parse_obj(registry_entry_dict)

    return registry_entry, connector_type


def get_connector_type_from_registry_entry(registry_entry: dict) -> TaggedRegistryEntry:
    if registry_entry.get("sourceDefinitionId"):
        return ("source", ConnectorRegistrySourceDefinition)
    elif registry_entry.get("destinationDefinitionId"):
        return ("destination", ConnectorRegistryDestinationDefinition)
    else:
        raise Exception("Could not determine connector type from registry entry")


def get_registry_entry_write_path(metadata_entry: LatestMetadataEntry, registry_name: str):
    metadata_path = metadata_entry.file_path
    if metadata_path is None:
        raise Exception(f"Metadata entry {metadata_entry} does not have a file path")

    metadata_folder = os.path.dirname(metadata_path)
    return os.path.join(metadata_folder, registry_name)


@sentry_sdk.trace
def persist_registry_entry_to_json(
    registry_entry: PolymorphicRegistryEntry,
    registry_name: str,
    metadata_entry: LatestMetadataEntry,
    registry_directory_manager: GCSFileManager,
) -> GCSFileHandle:
    """Persist the registry_entry to a json file on GCS bucket

    Args:
        registry_entry (ConnectorRegistryV0): The registry_entry.
        registry_name (str): The name of the registry_entry. One of "cloud" or "oss".
        metadata_entry (LatestMetadataEntry): The related Metadata Entry.
        registry_directory_manager (GCSFileHandle): The registry_entry directory manager.

    Returns:
        GCSFileHandle: The registry_entry directory manager.
    """
    registry_entry_write_path = get_registry_entry_write_path(metadata_entry, registry_name)
    registry_entry_json = registry_entry.json(exclude_none=True)
    file_handle = registry_directory_manager.write_data(registry_entry_json.encode("utf-8"), ext="json", key=registry_entry_write_path)
    HACKS.write_registry_to_overrode_file_paths(registry_entry, registry_name, metadata_entry, registry_directory_manager)
    return file_handle


@sentry_sdk.trace
def generate_and_persist_registry_entry(
    metadata_entry: LatestMetadataEntry,
    cached_specs: OutputDataFrame,
    metadata_directory_manager: GCSFileManager,
    registry_name: str,
) -> str:
    """Generate the selected registry from the metadata files, and persist it to GCS.

    Args:
        context (OpExecutionContext): The execution context.
        metadata_entry (List[LatestMetadataEntry]): The metadata definitions.
        cached_specs (OutputDataFrame): The cached specs.

    Returns:
        Output[ConnectorRegistryV0]: The registry.
    """
    raw_entry_dict = metadata_to_registry_entry(metadata_entry, registry_name)
    registry_entry_with_spec = apply_spec_to_registry_entry(raw_entry_dict, cached_specs)

    _, ConnectorModel = get_connector_type_from_registry_entry(registry_entry_with_spec)

    registry_model = ConnectorModel.parse_obj(registry_entry_with_spec)

    file_handle = persist_registry_entry_to_json(registry_model, registry_name, metadata_entry, metadata_directory_manager)

    return file_handle.public_url


def get_registry_status_lists(registry_entry: LatestMetadataEntry) -> Tuple[List[str], List[str]]:
    """Get the enabled registries for the given metadata entry.

    Args:
        registry_entry (LatestMetadataEntry): The metadata entry.

    Returns:
        Tuple[List[str], List[str]]: The enabled and disabled registries.
    """
    metadata_data_dict = registry_entry.metadata_definition.dict()

    # get data.registries fiield, handling the case where it is not present or none
    registries_field = get(metadata_data_dict, "data.registries") or {}

    # registries is a dict of registry_name -> {enabled: bool}
    all_enabled_registries = [
        registry_name for registry_name, registry_data in registries_field.items() if registry_data and registry_data.get("enabled")
    ]

    valid_enabled_registries = [registry_name for registry_name in all_enabled_registries if registry_name in VALID_REGISTRIES]

    valid_disabled_registries = [registry_name for registry_name in VALID_REGISTRIES if registry_name not in all_enabled_registries]

    return valid_enabled_registries, valid_disabled_registries


def delete_registry_entry(registry_name, registry_entry: LatestMetadataEntry, metadata_directory_manager: GCSFileManager) -> str:
    """Delete the given registry entry from GCS.

    Args:
        registry_entry (LatestMetadataEntry): The registry entry.
        metadata_directory_manager (GCSFileManager): The metadata directory manager.
    """
    registry_entry_write_path = get_registry_entry_write_path(registry_entry, registry_name)
    file_handle = metadata_directory_manager.delete_by_key(key=registry_entry_write_path, ext="json")
    return file_handle.public_url if file_handle else None


@sentry_sdk.trace
def safe_parse_metadata_definition(metadata_blob: storage.Blob) -> Optional[MetadataDefinition]:
    """
    Safely parse the metadata definition from the given metadata entry.
    Handles the case where the metadata definition is invalid for in old versions of the metadata.
    """
    yaml_string = metadata_blob.download_as_string().decode("utf-8")
    metadata_dict = yaml.safe_load(yaml_string)
    try:
        return MetadataDefinition.parse_obj(metadata_dict)

    except ValidationError as e:
        # only raise the error if "latest" is in the path
        if "latest" in metadata_blob.name:
            raise e
        else:
            print(f"WARNING: Could not parse metadata definition for {metadata_blob.name}. Error: {e}")
            return None


# ASSETS


@asset(
    required_resource_keys={"slack", "all_metadata_file_blobs"},
    group_name=GROUP_NAME,
    partitions_def=metadata_partitions_def,
    output_required=False,
    auto_materialize_policy=AutoMaterializePolicy.eager(max_materializations_per_minute=MAX_METADATA_PARTITION_RUN_REQUEST),
)
@sentry.instrument_asset_op
def metadata_entry(context: OpExecutionContext) -> Output[Optional[LatestMetadataEntry]]:
    """Parse and compute the LatestMetadataEntry for the given metadata file."""
    etag = context.partition_key
    context.log.info(f"Processing metadata file with etag {etag}")
    all_metadata_file_blobs = context.resources.all_metadata_file_blobs

    # find the blob with the matching etag
    matching_blob = next((blob for blob in all_metadata_file_blobs if blob.etag == etag), None)
    if not matching_blob:
        raise Exception(f"Could not find blob with etag {etag}")

    metadata_file_path = matching_blob.name
    PublishConnectorLifecycle.log(
        context,
        PublishConnectorLifecycleStage.METADATA_VALIDATION,
        StageStatus.IN_PROGRESS,
        f"Found metadata file with path {metadata_file_path} for etag {etag}",
    )

    # read the matching_blob into a metadata definition
    metadata_def = safe_parse_metadata_definition(matching_blob)

    dagster_metadata = {
        "bucket_name": matching_blob.bucket.name,
        "file_path": metadata_file_path,
        "partition_key": etag,
        "invalid_metadata": metadata_def is None,
    }

    # return only if the metadata definition is valid
    if not metadata_def:
        PublishConnectorLifecycle.log(
            context,
            PublishConnectorLifecycleStage.METADATA_VALIDATION,
            StageStatus.FAILED,
            f"Could not parse metadata definition for {metadata_file_path}, dont panic, this can be expected for old metadata files",
        )
        return Output(value=None, metadata=dagster_metadata)

    icon_file_path = metadata_file_path.replace(METADATA_FILE_NAME, ICON_FILE_NAME)
    icon_blob = matching_blob.bucket.blob(icon_file_path)

    icon_url = (
        get_public_url_for_gcs_file(icon_blob.bucket.name, icon_blob.name, os.getenv("METADATA_CDN_BASE_URL"))
        if icon_blob.exists()
        else None
    )

    metadata_entry = LatestMetadataEntry(
        metadata_definition=metadata_def,
        icon_url=icon_url,
        bucket_name=matching_blob.bucket.name,
        file_path=metadata_file_path,
    )

    PublishConnectorLifecycle.log(
        context,
        PublishConnectorLifecycleStage.METADATA_VALIDATION,
        StageStatus.SUCCESS,
        f"Successfully parsed metadata definition for {metadata_file_path}",
    )

    return Output(value=metadata_entry, metadata=dagster_metadata)


@asset(
    required_resource_keys={"slack", "root_metadata_directory_manager"},
    group_name=GROUP_NAME,
    partitions_def=metadata_partitions_def,
    auto_materialize_policy=AutoMaterializePolicy.eager(max_materializations_per_minute=MAX_METADATA_PARTITION_RUN_REQUEST),
)
@sentry.instrument_asset_op
def registry_entry(context: OpExecutionContext, metadata_entry: Optional[LatestMetadataEntry]) -> Output[Optional[dict]]:
    """
    Generate the registry entry files from the given metadata file, and persist it to GCS.
    """
    if not metadata_entry:
        # if the metadata entry is invalid, return an empty dict
        return Output(metadata={"empty_metadata": True}, value=None)

    PublishConnectorLifecycle.log(
        context,
        PublishConnectorLifecycleStage.REGISTRY_ENTRY_GENERATION,
        StageStatus.IN_PROGRESS,
        f"Generating registry entry for {metadata_entry.file_path}",
    )

    cached_specs = pd.DataFrame(list_cached_specs())

    root_metadata_directory_manager = context.resources.root_metadata_directory_manager
    enabled_registries, disabled_registries = get_registry_status_lists(metadata_entry)

    persisted_registry_entries = {
        registry_name: generate_and_persist_registry_entry(metadata_entry, cached_specs, root_metadata_directory_manager, registry_name)
        for registry_name in enabled_registries
    }

    deleted_registry_entries = {
        registry_name: delete_registry_entry(registry_name, metadata_entry, root_metadata_directory_manager)
        for registry_name in disabled_registries
    }

    dagster_metadata_persist = {
        f"create_{registry_name}": MetadataValue.url(registry_url) for registry_name, registry_url in persisted_registry_entries.items()
    }

    dagster_metadata_delete = {
        f"delete_{registry_name}": MetadataValue.url(registry_url) for registry_name, registry_url in deleted_registry_entries.items()
    }

    dagster_metadata = {
        **dagster_metadata_persist,
        **dagster_metadata_delete,
    }

    # Log the registry entries that were created
    for registry_name, registry_url in persisted_registry_entries.items():
        PublishConnectorLifecycle.log(
            context,
            PublishConnectorLifecycleStage.REGISTRY_ENTRY_GENERATION,
            StageStatus.SUCCESS,
            f"Successfully generated {registry_name} registry entry for {metadata_entry.file_path} at {registry_url}",
        )

    # Log the registry entries that were deleted
    for registry_name, registry_url in deleted_registry_entries.items():
        PublishConnectorLifecycle.log(
            context,
            PublishConnectorLifecycleStage.REGISTRY_ENTRY_GENERATION,
            StageStatus.SUCCESS,
            f"Successfully deleted {registry_name} registry entry for {metadata_entry.file_path}",
        )

    return Output(metadata=dagster_metadata, value=persisted_registry_entries)
