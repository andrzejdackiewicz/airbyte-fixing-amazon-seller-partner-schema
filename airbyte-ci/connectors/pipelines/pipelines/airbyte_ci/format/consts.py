#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from enum import Enum

REPO_MOUNT_PATH = "/src"
CACHE_MOUNT_PATH = "/cache"

LICENSE_FILE_NAME = "LICENSE_SHORT"

DEFAULT_FORMAT_IGNORE_LIST = [
    "**/__pycache__",
    '"**/.pytest_cache',
    "**/.venv",
    "**/venv",
    "**/.gradle",
    "**/node_modules",
    "**/.tox",
    "**/.eggs",
    "**/.mypy_cache",
    "**/.venv",
    "**/*.egg-info",
    "**/build",
    "**/dbt-project-template",
    "**/dbt-project-template-mssql",
    "**/dbt-project-template-mysql",
    "**/dbt-project-template-oracle",
    "**/dbt-project-template-clickhouse",
    "**/dbt-project-template-snowflake",
    "**/dbt-project-template-tidb",
    "**/dbt-project-template-duckdb",
    "**/dbt_test_config",
    "**/normalization_test_output",
    # '**/tools',
    "**/secrets",
    "**/charts",  # Helm charts often have injected template strings that will fail general linting. Helm linting is done separately.
    "**/resources/seed/*_catalog.json",  # Do not remove - this is also necessary to prevent diffs in our github workflows
    "**/resources/seed/*_registry.json",  # Do not remove - this is also necessary to prevent diffs in our github workflows
    "**/resources/seed/specs_secrets_mask.yaml",  # Downloaded externally.
    "**/resources/examples/airflow/superset/docker/pythonpath_dev/superset_config.py",
    "**/source-amplitude/unit_tests/api_data/zipped.json",  # Zipped file presents as non-UTF-8 making spotless sad
    "**/airbyte-connector-builder-server/connector_builder/generated",  # autogenerated code doesn't need to be formatted
    "**/airbyte-ci/connectors/metadata_service/lib/tests/fixtures/**/invalid",  # These are deliberately invalid and unformattable.
    "**/__init__.py",
    "**/declarative_component_schema.py",
    "**/source-stock-ticker-api-tutorial/source.py",
    "**/tools/git_hooks/tests/test_spec_linter.py",
    "**/tools/schema_generator/schema_generator/infer_schemas.py",
    "**/.git",
    "airbyte-ci/connectors/pipelines/tests/test_format/non_formatted_code",  # This is a test directory with badly formatted code
]


class Formatter(Enum):
    """An enum for the formatter values which can be ["java", "js", "python", "license"]."""

    JAVA = "java"
    JS = "js"
    PYTHON = "python"
    LICENSE = "license"


WARM_UP_INCLUSIONS = {
    Formatter.JAVA: [
        "spotless-maven-pom.xml",
        "tools/gradle/codestyle/java-google-style.xml",
    ],
    Formatter.PYTHON: ["pyproject.toml", "poetry.lock"],
    Formatter.LICENSE: [LICENSE_FILE_NAME],
}
