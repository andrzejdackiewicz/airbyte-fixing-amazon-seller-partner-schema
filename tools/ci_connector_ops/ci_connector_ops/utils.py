#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
import logging
from typing import Dict, List, Optional, Tuple

import git
import requests
import yaml

AIRBYTE_REPO = git.Repo(".")
OSS_CATALOG_URL = "https://storage.googleapis.com/prod-airbyte-cloud-connector-metadata-service/oss_catalog.json"
CONNECTOR_PATH_PREFIX = "airbyte-integrations/connectors"
SOURCE_CONNECTOR_PATH_PREFIX = CONNECTOR_PATH_PREFIX + "/source-"
ACCEPTANCE_TEST_CONFIG_FILE_NAME = "acceptance-test-config.yml"
AIRBYTE_DOCKER_REPO = "airbyte"


def download_catalog(catalog_url):
    response = requests.get(catalog_url)
    return response.json()


OSS_CATALOG = download_catalog(OSS_CATALOG_URL)


def get_changed_connector_names() -> List[str]:
    """Retrieve a list of connector names that were changed in the current branch (compared to master).

    Returns:
        List[str]: List of connector names e.g ["source-pokeapi"]
    """
    head_commit = AIRBYTE_REPO.head.commit
    master_diffs = head_commit.diff(AIRBYTE_REPO.remotes.origin.refs.master)
    changed_source_connector_files = [diff.b_path for diff in master_diffs if diff.b_path.startswith(SOURCE_CONNECTOR_PATH_PREFIX)]

    def get_connector_name_from_path(path):
        return path.split("/")[2]

    return [get_connector_name_from_path(changed_file) for changed_file in changed_source_connector_files]


def get_connector_definition(connector_name: str, catalog: Dict = OSS_CATALOG) -> Optional[Dict]:
    """Find a connector definition from the catalog.

    Args:
        connector_name (str): The connector name. E.G. 'source-pokeapi'
        catalog (Dict, optional): The connector catalog. Defaults to OSS_CATALOG.

    Raises:
        Exception: Raised if the definition type (source/destination) could not be determined from connector name.

    Returns:
        Optional[Dict]: The definition if the connector was found in the catalo. Returns None otherwise.
    """
    try:
        definition_type = connector_name.split("-")[0]
        assert definition_type in ["source", "destination"]
    except AssertionError:
        raise Exception(f"Could not determine the definition type for {connector_name}.")
    for definition in catalog[definition_type + "s"]:
        if definition["dockerRepository"].replace(f"{AIRBYTE_DOCKER_REPO}/", "") == connector_name:
            return definition


def get_connector_release_stage(connector_name: str, catalog: Dict = OSS_CATALOG) -> Optional[str]:
    """Retrieve the connector release stage (E.G. alpha/beta/generally_available).

    Args:
        connector_name (str): The connector name. E.G. 'source-pokeapi'
        catalog (Dict, optional): The connector catalog. Defaults to OSS_CATALOG.

    Returns:
        Optional[str]: The connector release stage if it was defined. Returns None otherwise.
    """
    definition = get_connector_definition(connector_name, catalog)
    return definition.get("releaseStage")


def get_acceptance_test_config(connector_name: str) -> Tuple[str, Dict]:
    """Retrieve the acceptance test config file path and its content as dict.

    Args:
        connector_name (str): The connector name. E.G. 'source-pokeapi'


    Returns:
        Tuple(str, Dict): The acceptance test config file path and its content as dict.
    """
    acceptance_test_config_path = f"{CONNECTOR_PATH_PREFIX}/{connector_name}/{ACCEPTANCE_TEST_CONFIG_FILE_NAME}"
    try:
        with open(acceptance_test_config_path) as acceptance_test_config_file:
            return acceptance_test_config_path, yaml.safe_load(acceptance_test_config_file)
    except FileNotFoundError:
        logging.warning(f"No {ACCEPTANCE_TEST_CONFIG_FILE_NAME} file found for {connector_name}")
        return None, None
