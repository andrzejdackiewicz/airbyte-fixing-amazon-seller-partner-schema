#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import uuid
from pathlib import Path
from typing import Optional, List
from dataclasses import dataclass
import dagger
from pipelines import main_logger
from pipelines.actions.environments import with_pip_packages, with_poetry_module, with_python_base, with_installed_pipx_package
from pipelines.bases import Report, Step, StepResult
from pipelines.contexts import PipelineContext, ConnectorContext
from pipelines.helpers.steps import run_steps
from pipelines.utils import DAGGER_CONFIG, get_secret_host_variable

METADATA_DIR = "airbyte-ci/connectors/metadata_service"
METADATA_LIB_MODULE_PATH = "lib"
METADATA_SERVICE_TOOL_MODULE_PATH = Path(f"{METADATA_DIR}/{METADATA_LIB_MODULE_PATH}")
METADATA_ORCHESTRATOR_MODULE_PATH = "orchestrator"


@dataclass(frozen=True)
class MountPath:
    path: Path
    optional: bool = False

    def __post_init__(self):
        if not self.path.exists():
            message = f"{self.path} does not exist."
            if self.optional:
                raise FileNotFoundError(message)
            else:
                main_logger.warning(message)

    def __str__(self):
        return str(self.path)

    @property
    def is_file(self) -> bool:
        return self.path.is_file()


# STEPS

class SimpleCIStep(Step):
    """A step that runs a given command in a container."""
    def __init__(
            self,
            title: str,
            context: PipelineContext,
            paths_to_mount: List[MountPath] = [],
            internal_tools: List[MountPath] = [],
            secrets: dict[str, dagger.Secret] = {},
            env_variables: dict[str, str] = {},
            working_directory: str = "/",
            command: Optional[List[str]] = None,
        ):
        """A step that runs a given command in a container.

        Args:
            title (str): name of the step
            context (PipelineContext): context of the step
            paths_to_mount (List[MountPath], optional): directory paths to mount. Defaults to [].
            internal_tools (List[MountPath], optional): internal tools to install. Defaults to [].
            secrets (dict[str, dagger.Secret], optional): secrets to add to container. Defaults to {}.
            env_variables (dict[str, str], optional): env variables to set in container. Defaults to {}.
            working_directory (str, optional): working director to run the command in. Defaults to "/".
            command (Optional[List[str]], optional): The default command to run. Defaults to None.
        """
        self.title = title
        super().__init__(context)

        self.paths_to_mount = paths_to_mount
        self.working_directory = working_directory
        self.internal_tools = internal_tools
        self.secrets = secrets
        self.env_variables = env_variables
        self.command = command

    def _mount_paths(self, container: dagger.Container) -> dagger.Container:
        for path_to_mount in self.paths_to_mount:
            path_string = str(path_to_mount)
            destination_path = f"/{path_string}"
            if path_to_mount.is_file:
                file_to_load = self.context.get_repo_file(path_string)
                container = container.with_mounted_file(destination_path, file_to_load)
            else:
                container = container.with_mounted_directory(destination_path, self.context.get_repo_dir(path_string))
        return container

    async def _install_internal_tools(self, container: dagger.Container) -> dagger.Container:
        for internal_tool in self.internal_tools:
            container = await with_installed_pipx_package(self.context, container, str(internal_tool))
        return container

    def _set_workdir(self, container: dagger.Container) -> dagger.Container:
        return container.with_workdir(self.working_directory)

    def _set_env_variables(self, container: dagger.Container) -> dagger.Container:
        for key, value in self.env_variables.items():
            container = container.with_env_variable(key, value)
        return container

    def _set_secrets(self, container: dagger.Container) -> dagger.Container:
        for key, value in self.secrets.items():
            container = container.with_secret_variable(key, value)
        return container

    async def init_container(self) -> dagger.Container:
        # TODO (ben): Replace with python base container when available
        container = with_python_base(self.context)

        container = self._mount_paths(container)
        container = self._set_env_variables(container)
        container = self._set_secrets(container)
        container = await self._install_internal_tools(container)
        container = self._set_workdir(container)

        return container

    async def _run(self, command = None) -> StepResult:
        command_to_run = command or self.command
        if not command_to_run:
            raise ValueError("No command to run")

        container_to_run = await self.init_container()
        return await self.get_step_result(container_to_run.with_exec(command_to_run))


class MetadataValidation(SimpleCIStep):
    def __init__(self, context: ConnectorContext):
        super().__init__(
            title=f"Validate metadata for {context.connector.technical_name}",
            context=context,
            paths_to_mount=[
                MountPath(context.connector.metadata_file_path),
                MountPath(context.connector.documentation_file_path),
                MountPath(context.connector.icon_path, optional=True),
            ],
            internal_tools=[
                MountPath(METADATA_SERVICE_TOOL_MODULE_PATH),
            ],
            command=["metadata_service", "validate", str(context.connector.metadata_file_path), str(context.connector.documentation_file_path)],
        )

class MetadataUpload(SimpleCIStep):
    # When the metadata service exits with this code, it means the metadata is valid but the upload was skipped because the metadata is already uploaded
    skipped_exit_code = 5

    def __init__(
        self,
        context: ConnectorContext,
        metadata_bucket_name: str,
        metadata_service_gcs_credentials_secret: dagger.Secret,
        docker_hub_username_secret: dagger.Secret,
        docker_hub_password_secret: dagger.Secret,
        pre_release: bool = False,
        pre_release_tag: Optional[str] = None,
    ):
        title = f"Upload metadata for {context.connector.technical_name} v{context.connector.docker_image_tag}"
        command_to_run = [
            "metadata_service",
            "upload",
            str(context.connector.metadata_file_path),
            str(context.connector.documentation_file_path),
            metadata_bucket_name
        ]

        if pre_release:
            command_to_run += ["--prerelease", pre_release_tag]

        super().__init__(
            title=title,
            context=context,
            paths_to_mount=[
                MountPath(context.connector.metadata_file_path),
                MountPath(context.connector.documentation_file_path),
                MountPath(context.connector.icon_path, optional=True),
            ],
            internal_tools=[
                MountPath(METADATA_SERVICE_TOOL_MODULE_PATH),
            ],
            secrets={
                "DOCKER_HUB_USERNAME": docker_hub_username_secret,
                "DOCKER_HUB_PASSWORD": docker_hub_password_secret,
                "GCS_CREDENTIALS": metadata_service_gcs_credentials_secret,
            },
            env_variables={
                # The cache buster ensures we always run the upload command (in case of remote bucket change)
                "CACHEBUSTER": str(uuid.uuid4()),
            },
            command=command_to_run,
        )


class PoetryRun(Step):
    def __init__(self, context: PipelineContext, title: str, parent_dir_path: str, module_path: str):
        self.title = title
        super().__init__(context)

        parent_dir = self.context.get_repo_dir(parent_dir_path)
        module_path = module_path
        self.poetry_run_container = with_poetry_module(self.context, parent_dir, module_path).with_entrypoint(["poetry", "run"])

    async def _run(self, poetry_run_args: list) -> StepResult:
        poetry_run_exec = self.poetry_run_container.with_exec(poetry_run_args)
        return await self.get_step_result(poetry_run_exec)


class DeployOrchestrator(Step):
    title = "Deploy Metadata Orchestrator to Dagster Cloud"
    deploy_dagster_command = [
        "dagster-cloud",
        "serverless",
        "deploy-python-executable",
        "--location-name",
        "metadata_service_orchestrator",
        "--location-file",
        "dagster_cloud.yaml",
        "--organization",
        "airbyte-connectors",
        "--deployment",
        "prod",
        "--python-version",
        "3.9",
    ]

    async def _run(self) -> StepResult:
        parent_dir = self.context.get_repo_dir(METADATA_DIR)
        python_base = with_python_base(self.context, "3.9")
        python_with_dependencies = with_pip_packages(python_base, ["dagster-cloud==1.2.6", "pydantic==1.10.6", "poetry2setup==1.1.0"])
        dagster_cloud_api_token_secret: dagger.Secret = get_secret_host_variable(
            self.context.dagger_client, "DAGSTER_CLOUD_METADATA_API_TOKEN"
        )

        container_to_run = (
            python_with_dependencies.with_mounted_directory("/src", parent_dir)
            .with_secret_variable("DAGSTER_CLOUD_API_TOKEN", dagster_cloud_api_token_secret)
            .with_workdir(f"/src/{METADATA_ORCHESTRATOR_MODULE_PATH}")
            .with_exec(["/bin/sh", "-c", "poetry2setup >> setup.py"])
            .with_exec(self.deploy_dagster_command)
        )
        return await self.get_step_result(container_to_run)


class TestOrchestrator(PoetryRun):
    def __init__(self, context: PipelineContext):
        super().__init__(
            context=context,
            title="Test Metadata Orchestrator",
            parent_dir_path=METADATA_DIR,
            module_path=METADATA_ORCHESTRATOR_MODULE_PATH,
        )

    async def _run(self) -> StepResult:
        return await super()._run(["pytest"])


# PIPELINES

async def run_metadata_lib_test_pipeline(
    is_local: bool,
    git_branch: str,
    git_revision: str,
    gha_workflow_run_url: Optional[str],
    dagger_logs_url: Optional[str],
    pipeline_start_timestamp: Optional[int],
    ci_context: Optional[str],
) -> bool:
    metadata_pipeline_context = PipelineContext(
        pipeline_name="Metadata Service Lib Unit Test Pipeline",
        is_local=is_local,
        git_branch=git_branch,
        git_revision=git_revision,
        gha_workflow_run_url=gha_workflow_run_url,
        dagger_logs_url=dagger_logs_url,
        pipeline_start_timestamp=pipeline_start_timestamp,
        ci_context=ci_context,
    )

    async with dagger.Connection(DAGGER_CONFIG) as dagger_client:
        metadata_pipeline_context.dagger_client = dagger_client.pipeline(metadata_pipeline_context.pipeline_name)
        async with metadata_pipeline_context:
            test_lib_step = PoetryRun(
                context=metadata_pipeline_context,
                title="Test Metadata Service Lib",
                parent_dir_path=METADATA_DIR,
                module_path=METADATA_LIB_MODULE_PATH,
            )
            result = await test_lib_step.run(["pytest"])
            metadata_pipeline_context.report = Report(
                pipeline_context=metadata_pipeline_context, steps_results=[result], name="METADATA LIB TEST RESULTS"
            )

    return metadata_pipeline_context.report.success


async def run_metadata_orchestrator_test_pipeline(
    is_local: bool,
    git_branch: str,
    git_revision: str,
    gha_workflow_run_url: Optional[str],
    dagger_logs_url: Optional[str],
    pipeline_start_timestamp: Optional[int],
    ci_context: Optional[str],
) -> bool:
    metadata_pipeline_context = PipelineContext(
        pipeline_name="Metadata Service Orchestrator Unit Test Pipeline",
        is_local=is_local,
        git_branch=git_branch,
        git_revision=git_revision,
        gha_workflow_run_url=gha_workflow_run_url,
        dagger_logs_url=dagger_logs_url,
        pipeline_start_timestamp=pipeline_start_timestamp,
        ci_context=ci_context,
    )

    async with dagger.Connection(DAGGER_CONFIG) as dagger_client:
        metadata_pipeline_context.dagger_client = dagger_client.pipeline(metadata_pipeline_context.pipeline_name)
        async with metadata_pipeline_context:
            test_orch_step = TestOrchestrator(context=metadata_pipeline_context)
            result = await test_orch_step.run()
            metadata_pipeline_context.report = Report(
                pipeline_context=metadata_pipeline_context, steps_results=[result], name="METADATA ORCHESTRATOR TEST RESULTS"
            )

    return metadata_pipeline_context.report.success


async def run_metadata_orchestrator_deploy_pipeline(
    is_local: bool,
    git_branch: str,
    git_revision: str,
    gha_workflow_run_url: Optional[str],
    dagger_logs_url: Optional[str],
    pipeline_start_timestamp: Optional[int],
    ci_context: Optional[str],
) -> bool:
    metadata_pipeline_context = PipelineContext(
        pipeline_name="Metadata Service Orchestrator Unit Test Pipeline",
        is_local=is_local,
        git_branch=git_branch,
        git_revision=git_revision,
        gha_workflow_run_url=gha_workflow_run_url,
        dagger_logs_url=dagger_logs_url,
        pipeline_start_timestamp=pipeline_start_timestamp,
        ci_context=ci_context,
    )

    async with dagger.Connection(DAGGER_CONFIG) as dagger_client:
        metadata_pipeline_context.dagger_client = dagger_client.pipeline(metadata_pipeline_context.pipeline_name)

        async with metadata_pipeline_context:
            steps = [TestOrchestrator(context=metadata_pipeline_context), DeployOrchestrator(context=metadata_pipeline_context)]
            steps_results = await run_steps(steps)
            metadata_pipeline_context.report = Report(
                pipeline_context=metadata_pipeline_context, steps_results=steps_results, name="METADATA ORCHESTRATOR DEPLOY RESULTS"
            )
    return metadata_pipeline_context.report.success
