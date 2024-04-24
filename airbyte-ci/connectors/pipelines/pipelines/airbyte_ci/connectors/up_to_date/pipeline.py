#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

import re
from typing import TYPE_CHECKING

import dagger
from packaging.version import Version
from connector_ops.utils import ConnectorLanguage  # type: ignore
from pipelines.airbyte_ci.connectors.build_image.steps.python_connectors import BuildConnectorImages
from pipelines.airbyte_ci.connectors.bump_version.pipeline import AddChangelogEntry, BumpDockerImageTagInMetadata, get_bumped_version
from pipelines.airbyte_ci.connectors.consts import CONNECTOR_TEST_STEP_ID
from pipelines.airbyte_ci.connectors.context import ConnectorContext, PipelineContext
from pipelines.airbyte_ci.connectors.reports import ConnectorReport, Report
from pipelines.consts import LOCAL_BUILD_PLATFORM
from pipelines.dagger.actions.python.common import with_python_connector_installed
from pipelines.helpers.connectors.cdk_helpers import get_latest_python_cdk_version
from pipelines.helpers.execution.run_steps import STEP_TREE, StepToRun, run_steps
from pipelines.models.steps import Step, StepResult, StepStatus

if TYPE_CHECKING:
    from anyio import Semaphore

PACKAGE_NAME_PATTERN = r"^([a-zA-Z0-9_.\-]+)(?:\[(.*?)\])?([=~><!]=?[a-zA-Z0-9\.]+)?$"

POETRY_LOCK_FILE = "poetry.lock"
POETRY_TOML_FILE = "pyproject.toml"


class CheckIsPythonUpdateable(Step):
    """Check if the connector is a candidate for updates.
    Candidate conditions:
    - The connector is a Python connector.
    - The connector is a source connector.
    - The connector is using poetry.
    - The connector has a base image defined in the metadata.
    """

    context: ConnectorContext

    title = "Check if the connector is a candidate for updating."

    def __init__(self, context: PipelineContext) -> None:
        super().__init__(context)

    async def _run(self) -> StepResult:
        connector_dir_entries = await (await self.context.get_connector_dir()).entries()
        if self.context.connector.language not in [ConnectorLanguage.PYTHON, ConnectorLanguage.LOW_CODE]:
            return StepResult(
                step=self,
                status=StepStatus.SKIPPED,
                stderr="The connector is not a Python connector.",
            )
        if self.context.connector.connector_type != "source":
            return StepResult(
                step=self,
                status=StepStatus.SKIPPED,
                stderr="The connector is not a source connector.",
            )
        if POETRY_LOCK_FILE not in connector_dir_entries or POETRY_TOML_FILE not in connector_dir_entries:
            return StepResult(
                step=self,
                status=StepStatus.SKIPPED,
                stderr="The connector requires poetry.",
            )

        if not self.context.connector.metadata or not self.context.connector.metadata.get("connectorBuildOptions", {}).get("baseImage"):
            return StepResult(
                step=self,
                status=StepStatus.SKIPPED,
                stderr="The connector can't be updated because it does not have a base image defined in the metadata.",
            )

        return StepResult(
            step=self,
            status=StepStatus.SUCCESS,
        )


class UpdatePoetry(Step):
    context: ConnectorContext

    title = "Update versions of libraries in poetry."

    def __init__(self, context: PipelineContext) -> None:
        super().__init__(context)

    async def _run(self) -> StepResult:
        base_image_name = self.context.connector.metadata["connectorBuildOptions"]["baseImage"]
        base_container = self.dagger_client.container(platform=LOCAL_BUILD_PLATFORM).from_(base_image_name)
        connector_container = await with_python_connector_installed(
            self.context,
            base_container,
            str(self.context.connector.code_directory),
        )

        try:
            before_versions = await get_poetry_versions(connector_container)
            before_main = await get_poetry_versions(connector_container, only="main")

            current_cdk_version = before_versions.get("airbyte-cdk") or None

            if current_cdk_version:
                # We want the CDK pinned exactly so it also works as expected in PyAirbyte and other `pip` scenarios
                new_cdk_version = pick_airbyte_cdk_version(current_cdk_version, self.context)
                if new_cdk_version > current_cdk_version:
                    connector_container = await connector_container.with_exec(["poetry", "add", f"airbyte-cdk=={new_cdk_version}"])

            connector_container = await connector_container.with_exec(["poetry", "update"])
            poetry_update_output = await connector_container.stdout()
            self.logger.info(poetry_update_output)

            after_versions = await get_poetry_versions(connector_container)
            updated_cdk_version = after_versions.get("airbyte-cdk") or None
            self.logger.info(f"airbyte-cdk updates: {current_cdk_version or 'None'} -> {updated_cdk_version or 'None'}")

            # see what changed
            main_changeset = get_package_changes(before_main, after_versions)
            all_changeset = get_package_changes(before_versions, after_versions)
            for package, version in main_changeset.items():
                self.logger.info(f"Main {package} updates: {before_versions.get(package) or 'None'} -> {version or 'None'}")
            for package, version in all_changeset.items():
                if package not in main_changeset:
                    self.logger.info(f" Dev {package} updates: {before_versions.get(package) or 'None'} -> {version or 'None'}")

            if not main_changeset:
                self.logger.info("No main dependencies updated.")
                return StepResult(step=self, status=StepStatus.SKIPPED, stderr="No main dependencies updated.")

            await connector_container.file(POETRY_TOML_FILE).export(f"{self.context.connector.code_directory}/{POETRY_TOML_FILE}")
            self.logger.info(f"Generated {POETRY_TOML_FILE} for {self.context.connector.technical_name}")
            await connector_container.file(POETRY_LOCK_FILE).export(f"{self.context.connector.code_directory}/{POETRY_LOCK_FILE}")
            self.logger.info(f"Generated {POETRY_LOCK_FILE} for {self.context.connector.technical_name}")

        except dagger.ExecError as e:
            return StepResult(step=self, status=StepStatus.FAILURE, stderr=str(e))

        return StepResult(step=self, status=StepStatus.SUCCESS, output=all_changeset)


class RestoreOriginalState(Step):
    context: ConnectorContext

    title = "Restore original state"

    def __init__(self, context: ConnectorContext) -> None:
        super().__init__(context)
        self.pyproject_path = context.connector.code_directory / POETRY_TOML_FILE
        if self.pyproject_path.exists():
            self.original_pyproject = self.pyproject_path.read_text()
        self.poetry_lock_path = context.connector.code_directory / POETRY_LOCK_FILE
        if self.poetry_lock_path.exists():
            self.original_poetry_lock = self.poetry_lock_path.read_text()

    async def _run(self) -> StepResult:
        if self.original_pyproject:
            self.pyproject_path.write_text(self.original_pyproject)
            self.logger.info(f"Restored {POETRY_TOML_FILE} for {self.context.connector.technical_name}")
        if self.original_poetry_lock:
            self.poetry_lock_path.write_text(self.original_poetry_lock)
            self.logger.info(f"Restored {POETRY_LOCK_FILE} for {self.context.connector.technical_name}")

        return StepResult(
            step=self,
            status=StepStatus.SUCCESS,
        )


class RegressionTest(Step):
    """Run the regression test for the connector.
    We test that:
    - The original dependencies are installed in the new connector image.
    - The dev dependencies are not installed in the new connector image.
    - The connector spec command successfully.
    """

    context: ConnectorContext

    title = "Run regression test"

    async def _run(self, new_connector_container: dagger.Container) -> StepResult:
        try:
            await new_connector_container.with_exec(["spec"])
            await new_connector_container.with_mounted_file(
                "pyproject.toml", (await self.context.get_connector_dir(include=["pyproject.toml"])).file("pyproject.toml")
            ).with_exec(["poetry", "run", self.context.connector.technical_name, "spec"], skip_entrypoint=True)
        except dagger.ExecError as e:
            return StepResult(
                step=self,
                status=StepStatus.FAILURE,
                stderr=str(e),
            )
        return StepResult(
            step=self,
            status=StepStatus.SUCCESS,
        )


def pick_airbyte_cdk_version(current_version: Version, context: ConnectorContext) -> Version:
    latest = Version(get_latest_python_cdk_version())

    # TODO: could add more logic here for semantic and other known things

    # 0.84: where from airbyte_cdk.sources.deprecated is removed
    if context.connector.language == ConnectorLanguage.PYTHON and current_version < Version("0.84.0"):
        return Version("0.83.0")

    return latest


def get_package_changes(before_versions: dict[str, Version], after_versions: dict[str, Version]) -> dict[str, Version]:
    changes: dict[str, Version] = {}
    for package, before_version in before_versions.items():
        after_version = after_versions.get(package)
        if after_version and before_version < after_version:
            changes[package] = after_version
    return changes


async def get_poetry_versions(connector_container: dagger.Container, only: str | None = None) -> dict[str, Version]:
    # -T makes it only the top-level ones
    # poetry show -T --only main will jsut be the main dependecies
    command = ["poetry", "show", "-T"]
    if only:
        command.append("--only")
        command.append(only)
    poetry_show_result = await connector_container.with_exec(command).stdout()
    versions: dict[str, Version] = {}
    lines = poetry_show_result.strip().split("\n")
    for line in lines:
        parts = line.split(maxsplit=2)  # Use maxsplit to limit the split parts
        if len(parts) >= 2:
            package = parts[0]
            # Regex to find version-like patterns. saw case with (!) before version
            version_match = re.search(r"\d+\.\d+.*", parts[1])
            if version_match:
                version = version_match.group()
                versions[package] = Version(version)
    return versions


async def run_connector_up_to_date_pipeline(context: ConnectorContext, semaphore: "Semaphore") -> Report:
    restore_original_state = RestoreOriginalState(context)

    # TODO: could pipe in the new version from the command line
    should_bump = False
    if should_bump:
        new_version = get_bumped_version(context.connector.version, "patch")
    else:
        new_version = None

    context.targeted_platforms = [LOCAL_BUILD_PLATFORM]

    steps_to_run: STEP_TREE = []

    steps_to_run.append([StepToRun(id=CONNECTOR_TEST_STEP_ID.CHECK_UPDATE_CANDIDATE, step=CheckIsPythonUpdateable(context))])

    steps_to_run.append(
        [
            StepToRun(
                id=CONNECTOR_TEST_STEP_ID.UPDATE_POETRY,
                step=UpdatePoetry(context),
                depends_on=[CONNECTOR_TEST_STEP_ID.CHECK_UPDATE_CANDIDATE],
            )
        ]
    )

    steps_to_run.append(
        [StepToRun(id=CONNECTOR_TEST_STEP_ID.BUILD, step=BuildConnectorImages(context), depends_on=[CONNECTOR_TEST_STEP_ID.UPDATE_POETRY])]
    )

    steps_to_run.append(
        [
            StepToRun(
                id=CONNECTOR_TEST_STEP_ID.REGRESSION_TEST,
                step=RegressionTest(context),
                depends_on=[CONNECTOR_TEST_STEP_ID.BUILD],
                args=lambda results: {"new_connector_container": results[CONNECTOR_TEST_STEP_ID.BUILD].output[LOCAL_BUILD_PLATFORM]},
            )
        ]
    )

    if new_version:
        steps_to_run.append(
            [
                StepToRun(
                    id=CONNECTOR_TEST_STEP_ID.BUMP_METADATA_VERSION,
                    step=BumpDockerImageTagInMetadata(
                        context,
                        await context.get_repo_dir(include=[str(context.connector.code_directory)]),
                        new_version,
                        export_metadata=True,
                    ),
                    depends_on=[CONNECTOR_TEST_STEP_ID.REGRESSION_TEST],
                )
            ]
        )
        steps_to_run.append(
            [
                StepToRun(
                    id=CONNECTOR_TEST_STEP_ID.ADD_CHANGELOG_ENTRY,
                    step=AddChangelogEntry(
                        context,
                        await context.get_repo_dir(include=[str(context.connector.local_connector_documentation_directory)]),
                        new_version,
                        f"TODO: better message - Poetry update.",
                        "0",
                        export_docs=True,
                    ),
                    depends_on=[CONNECTOR_TEST_STEP_ID.REGRESSION_TEST],
                )
            ]
        )

    async with semaphore:
        async with context:
            try:
                result_dict = await run_steps(
                    runnables=steps_to_run,
                    options=context.run_step_options,
                )
            except Exception as e:
                await restore_original_state.run()
                raise e
            results = list(result_dict.values())
            if any(step_result.status is StepStatus.FAILURE for step_result in results):
                await restore_original_state.run()
            report = ConnectorReport(context, steps_results=results, name="TEST RESULTS")
            context.report = report

    return report
