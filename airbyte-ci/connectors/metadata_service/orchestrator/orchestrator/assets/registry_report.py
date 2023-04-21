import pandas as pd
from dagster import MetadataValue, Output, asset
from typing import List
from orchestrator.templates.render import render_connector_registry_locations_html
from orchestrator.config import CONNECTOR_REPO_NAME, CONNECTORS_TEST_RESULT_BUCKET_URL
from orchestrator.resources.gcp import get_public_url_for_gcs_file_handle
import urllib.parse

GROUP_NAME = "registry_reports"

OSS_SUFFIX = "_oss"
CLOUD_SUFFIX = "_cloud"


# 🔗 HTML Renderers


def simple_link_html(url):
    if url:
        return f'<a href="{url}" target="_blank">🔗 Link</a>'
    else:
        return None


def icon_image_html(icon):
    github_icon_base = (
        f"https://raw.githubusercontent.com/{CONNECTOR_REPO_NAME}/master/airbyte-config-oss/init-oss/src/main/resources/icons"
    )
    icon_size = "30"
    icon_link = f'<img src="{github_icon_base}/{icon}" height="{icon_size}" height="{icon_size}"/>' if icon else "x"
    return icon_link


def test_badge_html(test_summary_url):
    if not test_summary_url:
        return None

    image_shield_base = "https://img.shields.io/endpoint"
    test_summary_url_encoded = urllib.parse.quote(test_summary_url)
    return f'<img src="{image_shield_base}?url={test_summary_url_encoded}">'


# 🖼️ Dataframe Columns


def github_url(docker_repo_name, github_connector_folders):
    if not isinstance(docker_repo_name, str):
        return None

    connector_name = docker_repo_name.replace("airbyte/", "")
    if connector_name in github_connector_folders:
        return f"https://github.com/{CONNECTOR_REPO_NAME}/blob/master/airbyte-integrations/connectors/{connector_name}"
    else:
        return None


def issue_url(row):
    docker_repo = row["dockerRepository_oss"]
    if not isinstance(docker_repo, str):
        print(f"no docker repo: {row}")
        return None

    code_name = docker_repo.split("/")[1]
    issues_label = (
        f"connectors/{'source' if 'source-' in code_name else 'destination'}/"
        f"{code_name.replace('source-', '').replace('destination-', '')}"
    )
    return f"https://github.com/{CONNECTOR_REPO_NAME}/issues?q=is:open+is:issue+label:{issues_label}"


def merge_docker_repo_and_version(row, suffix):
    docker_repo = row[f"dockerRepository{suffix}"]
    docker_version = row[f"dockerImageTag{suffix}"]

    if not isinstance(docker_repo, str):
        return None

    return f"{docker_repo}:{docker_version}"


def test_summary_url(row):
    docker_repo_name = row["dockerRepository_oss"]
    if not isinstance(docker_repo_name, str):
        return None

    connector = docker_repo_name.replace("airbyte/", "")

    return f"{CONNECTORS_TEST_RESULT_BUCKET_URL}/tests/summary/connectors/{connector}/badge.json"


# 📊 Dataframe Augmentation


def augment_and_normalize_connector_dataframes(
    cloud_df: pd.DataFrame, oss_df: pd.DataFrame, primaryKey: str, connector_type: str, github_connector_folders: List[str]
):
    """
    Normalize the cloud and oss connector dataframes and merge them into a single dataframe.
    Augment the dataframe with additional columns that indicate if the connector is in the cloud registry, oss registry, and if the metadata is valid.
    """

    # Add a column 'is_cloud' to indicate if an image/version pair is in the cloud registry
    cloud_df["is_cloud"] = True

    # Add a column 'is_oss' to indicate if an image/version pair is in the oss registry
    oss_df["is_oss"] = True

    # Merge the two registries on the 'image' and 'version' columns
    total_registry = pd.merge(oss_df, cloud_df, how="outer", suffixes=(OSS_SUFFIX, CLOUD_SUFFIX), on=primaryKey)

    # remove duplicates from the merged dataframe
    total_registry = total_registry.drop_duplicates(subset=primaryKey, keep="first")

    # Replace NaN values in the 'is_cloud' and 'is_oss' columns with False
    total_registry[["is_cloud", "is_oss"]] = total_registry[["is_cloud", "is_oss"]].fillna(False)

    # Set connectorType to 'source' or 'destination'
    total_registry["connector_type"] = connector_type

    total_registry["github_url"] = total_registry["dockerRepository_oss"].apply(lambda x: github_url(x, github_connector_folders))

    total_registry["issue_url"] = total_registry.apply(issue_url, axis=1)
    total_registry["test_summary_url"] = total_registry.apply(test_summary_url, axis=1)

    # Merge docker repo and version into separate columns
    total_registry["docker_image_oss"] = total_registry.apply(lambda x: merge_docker_repo_and_version(x, OSS_SUFFIX), axis=1)
    total_registry["docker_image_cloud"] = total_registry.apply(lambda x: merge_docker_repo_and_version(x, CLOUD_SUFFIX), axis=1)
    total_registry["docker_images_match"] = total_registry.apply(lambda x: x["docker_image_oss"] == x["docker_image_cloud"], axis=1)

    # Rename column primaryKey to 'definitionId'
    total_registry.rename(columns={primaryKey: "definitionId"}, inplace=True)

    return total_registry


# Dataframe to HTML


def dataframe_to_table_html(df: pd.DataFrame, column_mapping: List[dict]) -> str:
    """
    Convert a dataframe to an HTML table.
    """

    # convert true and false to checkmarks and x's
    df.replace({True: "✅", False: "❌"}, inplace=True)

    title_mapping = {column_info["column"]: column_info["title"] for column_info in column_mapping}

    df.rename(columns=title_mapping, inplace=True)

    html_formatters = {column_info["title"]: column_info["formatter"] for column_info in column_mapping if "formatter" in column_info}

    columns = [column_info["title"] for column_info in column_mapping]

    return df.to_html(
        columns=columns, justify="left", formatters=html_formatters, escape=False, classes="styled-table", na_rep="❌", render_links=True
    )


# ASSETS

# TODO (ben): Update these assets to reference the new registry once deployed


@asset(group_name=GROUP_NAME)
def all_sources_dataframe(legacy_cloud_sources_dataframe, legacy_oss_sources_dataframe, github_connector_folders) -> pd.DataFrame:
    """
    Merge the cloud and oss sources registries into a single dataframe.
    """

    return augment_and_normalize_connector_dataframes(
        cloud_df=legacy_cloud_sources_dataframe,
        oss_df=legacy_oss_sources_dataframe,
        primaryKey="sourceDefinitionId",
        connector_type="source",
        github_connector_folders=github_connector_folders,
    )


@asset(group_name=GROUP_NAME)
def all_destinations_dataframe(
    legacy_cloud_destinations_dataframe, legacy_oss_destinations_dataframe, github_connector_folders
) -> pd.DataFrame:
    """
    Merge the cloud and oss destinations registries into a single dataframe.
    """

    return augment_and_normalize_connector_dataframes(
        cloud_df=legacy_cloud_destinations_dataframe,
        oss_df=legacy_oss_destinations_dataframe,
        primaryKey="destinationDefinitionId",
        connector_type="destination",
        github_connector_folders=github_connector_folders,
    )


@asset(required_resource_keys={"registry_report_directory_manager"}, group_name=GROUP_NAME)
def connector_registry_report(context, all_destinations_dataframe, all_sources_dataframe):
    """
    Generate a report of the connector registry.
    """

    report_file_name = "connector_registry_report"
    all_connectors_dataframe = pd.concat([all_destinations_dataframe, all_sources_dataframe])
    all_connectors_dataframe.reset_index(inplace=True)

    columns_to_show = [
        {
            "column": "definitionId",
            "title": "Definition Id",
        },
        {
            "column": "name_oss",
            "title": "Name",
        },
        {
            "column": "icon_oss",
            "title": "Icon",
            "formatter": icon_image_html,
        },
        {
            "column": "connector_type",
            "title": "Connector Type",
        },
        {
            "column": "releaseStage_oss",
            "title": "Release Stage",
        },
        {
            "column": "test_summary_url",
            "title": "Build Status",
            "formatter": test_badge_html,
        },
        {
            "column": "is_oss",
            "title": "OSS",
        },
        {
            "column": "is_cloud",
            "title": "Cloud",
        },
        {
            "column": "docker_image_oss",
            "title": "Docker Image OSS",
        },
        {
            "column": "docker_image_cloud",
            "title": "Docker Image Cloud",
        },
        {
            "column": "docker_images_match",
            "title": "OSS and Cloud Docker Images Match",
        },
        {
            "column": "github_url",
            "title": "Source",
            "formatter": simple_link_html,
        },
        {
            "column": "documentationUrl_oss",
            "title": "Docs",
            "formatter": simple_link_html,
        },
        {
            "column": "issue_url",
            "title": "Issues",
            "formatter": simple_link_html,
        },
    ]

    html_string = render_connector_registry_locations_html(
        destinations_table=dataframe_to_table_html(all_destinations_dataframe, columns_to_show),
        sources_table=dataframe_to_table_html(all_sources_dataframe, columns_to_show),
    )

    json_string = all_connectors_dataframe.to_json(orient="records")

    registry_report_directory_manager = context.resources.registry_report_directory_manager

    json_file_handle = registry_report_directory_manager.write_data(json_string.encode(), ext="json", key=report_file_name)
    html_file_handle = registry_report_directory_manager.write_data(html_string.encode(), ext="html", key=report_file_name)

    metadata = {
        "first_10_preview": MetadataValue.md(all_connectors_dataframe.head(10).to_markdown()),
        "json": MetadataValue.json(json_string),
        "json_gcs_url": MetadataValue.url(get_public_url_for_gcs_file_handle(json_file_handle)),
        "html_gcs_url": MetadataValue.url(get_public_url_for_gcs_file_handle(html_file_handle)),
    }
    return Output(metadata=metadata, value=html_file_handle)
