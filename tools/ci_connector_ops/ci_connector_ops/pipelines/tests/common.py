#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

"""This module groups steps made to run tests agnostics to a connector language."""

import json
import uuid
from typing import List

import asyncer
from ci_connector_ops.pipelines.actions import environments
from ci_connector_ops.pipelines.bases import Step, StepResult, StepStatus
from dagger import CacheSharingMode


class QaChecks(Step):
    title = "QA checks"

    async def _run(self) -> List[StepResult]:
        """Runs our QA checks on a connector.
        The QA checks are defined in this module:
        https://github.com/airbytehq/airbyte/blob/master/tools/ci_connector_ops/ci_connector_ops/qa_checks.py

        Args:
            context (ConnectorTestContext): The current test context, providing a connector object, a dagger client and a repository directory.
        Returns:
            List[StepResult]: Failure or success of the QA checks with stdout and stdout in a list.
        """
        ci_connector_ops = await environments.with_ci_connector_ops(self.context)
        ci_connector_ops = self.get_dagger_pipeline(ci_connector_ops)
        filtered_repo = self.context.get_repo_dir(
            include=[
                str(self.context.connector.code_directory),
                str(self.context.connector.documentation_file_path),
                str(self.context.connector.icon_path),
                "airbyte-config/init/src/main/resources/seed/source_definitions.yaml",
                "airbyte-config/init/src/main/resources/seed/destination_definitions.yaml",
            ],
        )
        qa_checks = (
            ci_connector_ops.with_mounted_directory("/airbyte", filtered_repo)
            .with_workdir("/airbyte")
            .with_exec(["run-qa-checks", f"connectors/{self.context.connector.technical_name}"])
        )
        return [await self.get_step_result(qa_checks)]


class AcceptanceTests(Step):
    title = "Acceptance tests"

    async def _run(self) -> StepResult:
        """Runs the acceptance test suite on a connector dev image.
        It's rebuilding the connector acceptance test image if the tag is :dev.
        It's building the connector under test dev image if the connector image is :dev in the acceptance test config.

        Returns:
            StepResult: Failure or success of the acceptances tests with stdout and stdout.
        """
        if not self.context.connector.acceptance_test_config:
            return StepResult(Step.ACCEPTANCE_TESTS, StepStatus.SKIPPED), None

        dagger_client = self.get_dagger_pipeline(self.context.dagger_client)

        if self.context.connector_acceptance_test_image.endswith(":dev"):
            cat_container = self.context.connector_acceptance_test_source_dir.docker_build()
        else:
            cat_container = dagger_client.container().from_(self.context.connector_acceptance_test_image)

        dockerd, docker_host, share_tmp_volume = environments.get_dind_container_and_host(dagger_client, "cat")

        acceptance_test_cache_buster = str(uuid.uuid4())
        if self.context.connector.acceptance_test_config["connector_image"].endswith(":dev"):
            docker_cli = environments.with_docker_cli(self.context, dockerd, await docker_host)
            inspect_output = await (
                docker_cli.with_mounted_directory("/connector_to_build", self.context.get_connector_dir(exclude=[".venv"]))
                .with_workdir("/connector_to_build")
                .with_exec(["docker", "build", ".", "-t", f"airbyte/{self.context.connector.technical_name}:dev"])
                .with_exec(["docker", "image", "inspect", f"airbyte/{self.context.connector.technical_name}:dev"])
                .stdout()
            )
            acceptance_test_cache_buster = json.loads(inspect_output)[0]["Id"]

        cat_container = environments.with_cat(self.context, dockerd, docker_host, share_tmp_volume, acceptance_test_cache_buster)
        secret_dir = cat_container.directory("/test_input/secrets")

        async with asyncer.create_task_group() as task_group:
            soon_secret_files = task_group.soonify(secret_dir.entries)()
            soon_cat_container_stdout = task_group.soonify(cat_container.stdout)()

        if secret_files := soon_secret_files.value:
            for file_path in secret_files:
                if file_path.startswith("updated_configurations"):
                    self.context.updated_secrets_dir = secret_dir
                    break

        return self.pytest_logs_to_step_result(soon_cat_container_stdout.value)
