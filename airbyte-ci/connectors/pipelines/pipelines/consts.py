#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import platform
from pathlib import Path

from dagger import Platform

PYPROJECT_TOML_FILE_PATH = "pyproject.toml"
LICENSE_SHORT_FILE_PATH = "LICENSE_SHORT"
CONNECTOR_TESTING_REQUIREMENTS = [
    "pip==21.3.1",
    "mccabe==0.6.1",
    "flake8==4.0.1",
    "pyproject-flake8==0.0.1a2",
    "black==22.3.0",
    "isort==5.6.4",
    "pytest==6.2.5",
    "coverage[toml]==6.3.1",
    "pytest-custom_exit_code",
    "licenseheaders==0.8.8",
]

CI_CREDENTIALS_SOURCE_PATH = "airbyte-ci/connectors/ci_credentials"
CONNECTOR_OPS_SOURCE_PATHSOURCE_PATH = "airbyte-ci/connectors/connector_ops"
BUILD_PLATFORMS = [Platform("linux/amd64"), Platform("linux/arm64")]
LOCAL_BUILD_PLATFORM = Platform(f"linux/{platform.machine()}")
AMAZONCORRETTO_TAG = "17.0.8-al2023"
PYTHON_DOCKER_VERSION = "24.0.2"
DOCKER_DIND_IMAGE = "docker:24-dind"
DOCKER_CLI_IMAGE = "docker:24-cli"
GRADLE_CACHE_PATH = "/root/.gradle/caches"
GRADLE_BUILD_CACHE_PATH = f"{GRADLE_CACHE_PATH}/build-cache-1"
GRADLE_READ_ONLY_DEPENDENCY_CACHE_PATH = "/root/gradle_dependency_cache"
LOCAL_REPORTS_PATH_ROOT = "airbyte-ci/connectors/pipelines/pipeline_reports/"
GCS_PUBLIC_DOMAIN = "https://storage.cloud.google.com"
