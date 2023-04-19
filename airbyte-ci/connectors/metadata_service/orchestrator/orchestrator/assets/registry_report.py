import pandas as pd
from dagster import MetadataValue, Output, asset

from orchestrator.templates.render import render_connector_registry_locations_html, render_connector_registry_locations_markdown

GROUP_NAME = "registry_reports"
OSS_SUFFIX = "_oss"
CLOUD_SUFFIX = "_cloud"

# HELPERS


def augment_and_normalize_connector_dataframes(
    cloud_df, oss_df, primaryKey, connector_type, github_connector_folders
):
    """
    Normalize the cloud and oss connector dataframes and merge them into a single dataframe.
    Augment the dataframe with additional columns that indicate if the connector is in the cloud registry, oss registry, and if the metadata is valid.
    """

    # Add a column 'is_cloud' to indicate if an image/version pair is in the cloud registry
    cloud_df["is_cloud"] = True

    # Add a column 'is_oss' to indicate if an image/version pair is in the oss registry
    oss_df["is_oss"] = True

    # composite_key = [primaryKey, "dockerRepository", "dockerImageTag"]

    # Merge the two registries on the 'image' and 'version' columns, keeping only the unique pairs
    total_registry = pd.merge(cloud_df, oss_df, how="outer", suffixes=(OSS_SUFFIX, CLOUD_SUFFIX), on=primaryKey)

    # Replace NaN values in the 'is_cloud' and 'is_oss' columns with False
    total_registry[["is_cloud", "is_oss"]] = total_registry[["is_cloud", "is_oss"]].fillna(False)

    # registry_with_metadata = pd.merge(
    #     total_registry,
    #     valid_metadata_report_dataframe[["definitionId", "is_metadata_valid"]],
    #     left_on=primaryKey,
    #     right_on="definitionId",
    #     how="left",
    # )

    # merge with cached_specs on dockerRepository and dockerImageTag
    # cached_specs["is_spec_cached"] = True
    # merged_registry = pd.merge(
    #     total_registry,
    #     cached_specs,
    #     left_on=["dockerRepository", "dockerImageTag"],
    #     right_on=["docker_repository", "docker_image_tag"],
    #     how="left",
    # )

    # Set connectorType to 'source' or 'destination'
    total_registry["connector_type"] = connector_type
    # total_registry["is_source_controlled"] = total_registry["dockerRepository_oss"].apply(
    #     lambda x: x.lstrip("airbyte/") in github_connector_folders
    # )

    # Rename column primaryKey to 'definitionId'
    total_registry.rename(columns={primaryKey: "definitionId"}, inplace=True)

    return total_registry


# ASSETS


# @asset(required_resource_keys={"registry_report_directory_manager"}, group_name=GROUP_NAME)
# def connector_registry_location_html(context, all_destinations_dataframe, all_sources_dataframe):
#     """
#     Generate an HTML report of the connector registry locations.
#     """

#     columns_to_show = [
#         "dockerRepository",
#         "dockerImageTag",
#         "is_oss",
#         "is_cloud",
#         "is_source_controlled",
#         "is_spec_cached",
#     ]

#     # convert true and false to checkmarks and x's
#     all_sources_dataframe.replace({True: "✅", False: "❌"}, inplace=True)
#     all_destinations_dataframe.replace({True: "✅", False: "❌"}, inplace=True)

#     html = render_connector_registry_locations_html(
#         destinations_table=all_destinations_dataframe[columns_to_show].to_html(),
#         sources_table=all_sources_dataframe[columns_to_show].to_html(),
#     )

#     registry_report_directory_manager = context.resources.registry_report_directory_manager
#     file_handle = registry_report_directory_manager.write_data(html.encode(), ext="html", key="connector_registry_locations")

#     metadata = {
#         "preview": html,
#         "gcs_path": MetadataValue.url(file_handle.gcs_path),
#     }

#     return Output(metadata=metadata, value=file_handle)


# @asset(required_resource_keys={"registry_report_directory_manager"}, group_name=GROUP_NAME)
# def connector_registry_location_markdown(context, all_destinations_dataframe, all_sources_dataframe):
#     """
#     Generate a markdown report of the connector registry locations.
#     """

#     columns_to_show = [
#         "dockerRepository",
#         "dockerImageTag",
#         "is_oss",
#         "is_cloud",
#         "is_source_controlled",
#         "is_spec_cached",
#     ]

#     # convert true and false to checkmarks and x's
#     all_sources_dataframe.replace({True: "✅", False: "❌"}, inplace=True)
#     all_destinations_dataframe.replace({True: "✅", False: "❌"}, inplace=True)

#     markdown = render_connector_registry_locations_markdown(
#         destinations_markdown=all_destinations_dataframe[columns_to_show].to_markdown(),
#         sources_markdown=all_sources_dataframe[columns_to_show].to_markdown(),
#     )

#     registry_report_directory_manager = context.resources.registry_report_directory_manager
#     file_handle = registry_report_directory_manager.write_data(markdown.encode(), ext="md", key="connector_registry_locations")

#     metadata = {
#         "preview": MetadataValue.md(markdown),
#         "gcs_path": MetadataValue.url(file_handle.gcs_path),
#     }
#     return Output(metadata=metadata, value=file_handle)


@asset(group_name=GROUP_NAME)
def all_sources_dataframe(
    cloud_sources_dataframe, oss_sources_dataframe, github_connector_folders
) -> pd.DataFrame:
    """
    Merge the cloud and oss sources registries into a single dataframe.
    """

    return augment_and_normalize_connector_dataframes(
        cloud_df=cloud_sources_dataframe,
        oss_df=oss_sources_dataframe,
        primaryKey="sourceDefinitionId",
        connector_type="source",
        github_connector_folders=github_connector_folders,
    )


@asset(group_name=GROUP_NAME)
def all_destinations_dataframe(
    cloud_destinations_dataframe, oss_destinations_dataframe, github_connector_folders
) -> pd.DataFrame:
    """
    Merge the cloud and oss destinations registries into a single dataframe.
    """

    return augment_and_normalize_connector_dataframes(
        cloud_df=cloud_destinations_dataframe,
        oss_df=oss_destinations_dataframe,
        primaryKey="destinationDefinitionId",
        connector_type="destination",
        github_connector_folders=github_connector_folders,
    )

@asset(required_resource_keys={"registry_report_directory_manager"}, group_name=GROUP_NAME)
def connector_registry_report(context, all_destinations_dataframe, all_sources_dataframe):
    """
    Generate a markdown report of the connector registry locations.
    """

    columns_to_show = [
        "definitionId",
        "name_oss",
        "icon_oss",
        "dockerRepository_oss",
        "dockerImageTag_oss",
        "dockerRepository_cloud",
        "dockerImageTag_cloud",
        "is_oss",
        "is_cloud",
        # "is_source_controlled",
        # "is_spec_cached",

        # build_status_badge
        # icon
        # id
        # name
        # cloud vs oss docker image
        # cloud vs oss docker version
        # Do they match??
        # CDK version
        # Release stage
        # docs
        # issues
        # repo
        # source
    ]

    # convert true and false to checkmarks and x's
    all_sources_dataframe.replace({True: "✅", False: "❌"}, inplace=True)
    all_destinations_dataframe.replace({True: "✅", False: "❌"}, inplace=True)

    markdown = render_connector_registry_locations_markdown(
        destinations_markdown=all_destinations_dataframe[columns_to_show].to_markdown(),
        sources_markdown=all_sources_dataframe[columns_to_show].to_markdown(),
    )

    registry_report_directory_manager = context.resources.registry_report_directory_manager
    file_handle = registry_report_directory_manager.write_data(markdown.encode(), ext="md", key="connector_registry_report")

    metadata = {
        "preview": MetadataValue.md(markdown),
        "gcs_path": MetadataValue.url(file_handle.gcs_path),
    }
    return Output(metadata=metadata, value=file_handle)

