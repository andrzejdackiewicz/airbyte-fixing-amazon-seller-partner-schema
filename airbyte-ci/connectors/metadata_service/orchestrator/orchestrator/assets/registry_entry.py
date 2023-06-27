import pandas as pd
import numpy as np
import os
from typing import List, Optional, Tuple, Union
from dagster import DynamicPartitionsDefinition, asset, OpExecutionContext, Output, MetadataValue
import yaml
from dagster_gcp.gcs.file_manager import GCSFileManager, GCSFileHandle

from metadata_service.models.generated.ConnectorMetadataDefinitionV0 import ConnectorMetadataDefinitionV0
from metadata_service.constants import METADATA_FILE_NAME, ICON_FILE_NAME

from orchestrator.utils.object_helpers import are_values_equal, merge_values
from orchestrator.models.metadata import PartialMetadataDefinition, MetadataDefinition, LatestMetadataEntry
from orchestrator.config import get_public_url_for_gcs_file

from orchestrator.utils.dagster_helpers import OutputDataFrame, output_dataframe
from metadata_service.models.generated.ConnectorRegistryDestinationDefinition import ConnectorRegistryDestinationDefinition
from metadata_service.models.generated.ConnectorRegistrySourceDefinition import ConnectorRegistrySourceDefinition
from orchestrator.utils.object_helpers import deep_copy_params
from metadata_service.spec_cache import get_cached_spec

import copy

GROUP_NAME = "registry_entry"

metadata_partitions_def = DynamicPartitionsDefinition(name="metadata")

# ERRORS

class MissingCachedSpecError(Exception):
    pass

# HELPERS

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

    return releases_or_breaking_change.get("migrationDocumentationUrl", default_migration_documentation_url)


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
    overrode_metadata_data = apply_overrides_from_registry(metadata_data, override_registry_key)

    # remove fields that are not needed in the registry
    del overrode_metadata_data["registries"]
    del overrode_metadata_data["connectorType"]

    # rename field connectorSubtype to sourceType
    connection_type = overrode_metadata_data.get("connectorSubtype")
    if connection_type:
        overrode_metadata_data["sourceType"] = overrode_metadata_data["connectorSubtype"]
        del overrode_metadata_data["connectorSubtype"]

    # rename definitionId field to sourceDefinitionId or destinationDefinitionId
    id_field = "sourceDefinitionId" if connector_type == "source" else "destinationDefinitionId"
    overrode_metadata_data[id_field] = overrode_metadata_data["definitionId"]
    del overrode_metadata_data["definitionId"]

    # add in useless fields that are currently required for porting to the actor definition spec
    overrode_metadata_data["tombstone"] = False
    overrode_metadata_data["custom"] = False
    overrode_metadata_data["public"] = True

    # if there is no releaseStage, set it to "alpha"
    if not overrode_metadata_data.get("releaseStage"):
        overrode_metadata_data["releaseStage"] = "alpha"

    # apply generated fields
    overrode_metadata_data["iconUrl"] = metadata_entry.icon_url
    overrode_metadata_data["releases"] = apply_connector_release_defaults(overrode_metadata_data)

    return overrode_metadata_data

def get_connector_type_from_registry_entry(registry_entry: dict) -> Tuple[str, Union[ConnectorRegistrySourceDefinition, ConnectorRegistryDestinationDefinition]]:
    if registry_entry.get("sourceDefinitionId"):
        return ("source", ConnectorRegistrySourceDefinition)
    elif registry_entry.get("destinationDefinitionId"):
        return ("destination", ConnectorRegistryDestinationDefinition)
    else:
        raise Exception("Could not determine connector type from registry entry")

def persist_registry_entry_to_json(
    registry_entry: Union[ConnectorRegistrySourceDefinition, ConnectorRegistryDestinationDefinition], registry_name: str, metadata_path: str, registry_directory_manager: GCSFileManager
) -> GCSFileHandle:
    """Persist the registry_entry to a json file on GCS bucket

    Args:
        registry_entry (ConnectorRegistryV0): The registry_entry.
        registry_name (str): The name of the registry_entry. One of "cloud" or "oss".
        metadata_path (str): The path to the metadata file.
        registry_directory_manager (OutputDataFrame): The registry_entry directory manager.

    Returns:
        OutputDataFrame: The registry_entry directory manager.
    """
    metadata_folder = os.path.dirname(metadata_path)
    print(f"metadata_folder: {metadata_folder}")


    registry_entry_write_path = os.path.join(metadata_folder, registry_name)
    registry_entry_json = registry_entry.json(exclude_none=True)
    file_handle = registry_directory_manager.write_data(registry_entry_json.encode("utf-8"), ext="json", key=registry_entry_write_path)
    return file_handle

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
    metadata_path = metadata_entry.file_path
    if metadata_path is None:
        raise Exception(f"Metadata entry {metadata_entry} does not have a file path")

    raw_entry_dict = metadata_to_registry_entry(metadata_entry, registry_name)
    registry_entry_with_spec = apply_spec_to_registry_entry(raw_entry_dict, cached_specs)

    _, ConnectorModel = get_connector_type_from_registry_entry(registry_entry_with_spec)

    registry_model = ConnectorModel.parse_obj(registry_entry_with_spec)

    file_handle = persist_registry_entry_to_json(registry_model, registry_name, metadata_path, metadata_directory_manager)
    return file_handle.public_url

def get_enabled_registries(registry_entry: LatestMetadataEntry) -> List[str]:
    """Get the enabled registries for the given metadata entry.

    Args:
        registry_entry (LatestMetadataEntry): The metadata entry.

    Returns:
        List[str]: The enabled registries.
    """
    metadata_data_dict = registry_entry.metadata_definition.dict()
    registries_field = metadata_data_dict["data"].get("registries", {})

    print(f"registries_field: {registries_field}")


    # registries is a dict of registry_name -> {enabled: bool}
    # we want to return a list of registry names where enabled is true
    enabled_registries = [
        registry_name
        for registry_name, registry_data in registries_field.items()
        if registry_data.get("enabled")
    ]

    return enabled_registries

# ASSETS

@asset(required_resource_keys={"latest_metadata_file_blobs"}, group_name=GROUP_NAME, partitions_def=metadata_partitions_def)
def metadata_entry(context: OpExecutionContext) -> LatestMetadataEntry:
    etag = context.partition_key
    latest_metadata_file_blobs = context.resources.latest_metadata_file_blobs

    # find the blob with the matching etag
    matching_blob = next(
        (blob for blob in latest_metadata_file_blobs if blob.etag == etag), None
    )

    if not matching_blob:
        raise Exception(f"Could not find blob with etag {etag}")

    # read the matching_blob
    yaml_string = matching_blob.download_as_string().decode("utf-8")
    metadata_dict = yaml.safe_load(yaml_string)
    metadata_def = MetadataDefinition.parse_obj(metadata_dict)

    metadata_file_path = matching_blob.name
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
    # import pdb; pdb.set_trace()
    return metadata_entry


@asset(required_resource_keys={"root_metadata_directory_manager"}, group_name=GROUP_NAME, partitions_def=metadata_partitions_def)
def registry_entry(context: OpExecutionContext, metadata_entry: LatestMetadataEntry, cached_specs: pd.DataFrame) -> Output[dict]:
    """
    TODO
    1. parse into the individual registry files
    2. update registry sensor to use these files
    3. update the metadata entry to span all the registry files
    """
    root_metadata_directory_manager = context.resources.root_metadata_directory_manager
    enabled_registries = get_enabled_registries(metadata_entry)
    persisted_registry_entries = {
        registry_name: generate_and_persist_registry_entry(metadata_entry, cached_specs, root_metadata_directory_manager, registry_name)
        for registry_name
        in enabled_registries
    }

    metadata = {
        registry_name: MetadataValue.url(registry_url)
        for registry_name, registry_url in persisted_registry_entries.items()
    }

    print(f"Enabled Registries: {enabled_registries}")
    print(f"Persisted registry entries: {persisted_registry_entries}")
    print(f"Persisted registry metadata: {metadata}")
    print(f"Persisted registry entries: {metadata}")

    return Output(metadata=metadata, value=persisted_registry_entries)


