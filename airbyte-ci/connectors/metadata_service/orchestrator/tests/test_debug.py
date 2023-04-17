from dagster import build_op_context

from orchestrator.resources.gcp import gcp_gcs_client, gcs_bucket_manager, gcs_file_manager, gcs_file_blob, gcs_directory_blobs
from orchestrator.resources.github import github_client, github_connector_repo, github_connectors_directory

from orchestrator.assets.registry import (
    oss_destinations_dataframe,
    cloud_destinations_dataframe,
    oss_sources_dataframe,
    cloud_sources_dataframe,
    latest_oss_registry_dict,
    latest_cloud_registry_dict,
    cloud_registry_from_metadata,
    oss_registry_from_metadata,
)
from orchestrator.assets.metadata import (
    registry_derived_metadata_definitions,
    metadata_definitions,
)


from orchestrator.config import REPORT_FOLDER, REGISTRIES_FOLDER, CONNECTORS_PATH, CONNECTOR_REPO_NAME
from metadata_service.constants import METADATA_FILE_NAME, METADATA_FOLDER


def debug_registry_projection():
    """
    This is a debug function that is used to test the registry projection end to end.

    This is currently the only way to be able to set breakpoints in the registry projection code.

    It is not intended to be used in production.
    """

    resources = {
        "github_client": github_client,
        "github_connector_repo": github_connector_repo.configured({"connector_repo_name": CONNECTOR_REPO_NAME}),
        "github_connectors_directory": github_connectors_directory.configured({"connectors_path": CONNECTORS_PATH}),
        "gcp_gcs_client": gcp_gcs_client.configured(
            {
                "gcp_gcs_cred_string": {"env": "GCS_CREDENTIALS"},
            }
        ),
        "gcs_bucket_manager": gcs_bucket_manager.configured({"gcs_bucket": {"env": "METADATA_BUCKET"}}),
        "registry_report_directory_manager": gcs_file_manager.configured(
            {"gcs_bucket": {"env": "METADATA_BUCKET"}, "prefix": REPORT_FOLDER}
        ),
        "latest_oss_registry_gcs_blob": gcs_file_blob.configured({"prefix": REGISTRIES_FOLDER, "gcs_filename": "oss_registry.json"}),
        "latest_cloud_registry_gcs_blob": gcs_file_blob.configured({"prefix": REGISTRIES_FOLDER, "gcs_filename": "cloud_registry.json"}),
    }

    context = build_op_context(resources=resources)
    cloud_registry_dict = latest_cloud_registry_dict(context)
    cloud_destinations_df = cloud_destinations_dataframe(cloud_registry_dict).value
    cloud_sources_df = cloud_sources_dataframe(cloud_registry_dict).value

    oss_registry_dict = latest_oss_registry_dict(context)
    oss_destinations_df = oss_destinations_dataframe(oss_registry_dict).value
    oss_sources_df = oss_sources_dataframe(oss_registry_dict).value
    # github_connector_folders_list = github_connector_folders(context).value

    registry_derived_metadata_definitions(context, cloud_sources_df, cloud_destinations_df, oss_sources_df, oss_destinations_df).value
    # valid_metadata_report_dataframe_df = valid_metadata_report_dataframe(metadata_definitions_df).value

    # all_sources_df = all_sources_dataframe(cloud_sources_df, oss_sources_df, github_connector_folders_list, valid_metadata_report_dataframe_df)
    # all_destinations_df = all_destinations_dataframe(cloud_destinations_df, oss_destinations_df)

    # connector_registry_location_html(context, all_sources_df, all_destinations_df)
    # connector_registry_location_markdown(context, all_sources_df, all_destinations_df)

def test_debug_registry_generation():
    resources = {
        "gcp_gcs_client": gcp_gcs_client.configured(
            {
                "gcp_gcs_cred_string": {"env": "GCS_CREDENTIALS"},
            }
        ),
        "gcs_bucket_manager": gcs_bucket_manager.configured({"gcs_bucket": {"env": "METADATA_BUCKET"}}),
        "registry_directory_manager": gcs_file_manager.configured({"gcs_bucket": {"env": "METADATA_BUCKET"}, "prefix": REGISTRY_FOLDER}),
        "metadata_file_blobs": gcs_directory_blobs.configured({"prefix": METADATA_FOLDER, "suffix": METADATA_FILE_NAME}),
    }

    context = build_op_context(resources=resources)
    metadata_definitions_asset = metadata_definitions(context)
    oss_registry_from_metadata(context, metadata_definitions_asset).value
    # cloud_registry_from_metadata(context, metadata_definitions_asset).value
