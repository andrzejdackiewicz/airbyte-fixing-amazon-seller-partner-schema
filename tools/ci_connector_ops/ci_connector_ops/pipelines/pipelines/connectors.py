#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
"""This module groups the functions to run full pipelines for connector testing."""

import itertools
from typing import List

import anyio
import asyncer
import dagger
from ci_connector_ops.pipelines import builds, tests
from ci_connector_ops.pipelines.bases import ConnectorTestReport
from ci_connector_ops.pipelines.contexts import ConnectorTestContext
from ci_connector_ops.pipelines.utils import DAGGER_CONFIG

# CONSTANTS

GITHUB_GLOBAL_CONTEXT = "[POC please ignore] Connectors CI"
GITHUB_GLOBAL_DESCRIPTION = "Running connectors tests"


# DAGGER PIPELINES


async def run_connector_test_pipeline(context: ConnectorTestContext, semaphore: anyio.Semaphore) -> ConnectorTestReport:
    """Run a test pipeline for a single connector.

    A visual DAG can be found on the README.md file of the pipelines modules.

    Args:
        context (ConnectorTestContext): The initialized connector test context.

    Returns:
        ConnectorTestReport: The test reports holding tests results.
    """
    async with semaphore:
        async with context:
            async with asyncer.create_task_group() as task_group:
                tasks = [
                    task_group.soonify(tests.run_qa_checks)(context),
                    task_group.soonify(tests.run_code_format_checks)(context),
                    task_group.soonify(tests.run_all_tests)(context),
                ]
            results = list(itertools.chain(*(task.value for task in tasks)))
            context.test_report = ConnectorTestReport(context, steps_results=results)

        return context.test_report


async def run_connectors_test_pipelines(contexts: List[ConnectorTestContext], concurrency: int = 5):
    """Run a CI pipeline for all the connectors passed.

    Args:
        contexts (List[ConnectorTestContext]): List of connector test contexts for which a CI pipeline needs to be run.
        concurrency (int): Number of test pipeline that can run in parallel. Defaults to 5
    """
    semaphore = anyio.Semaphore(concurrency)
    async with dagger.Connection(DAGGER_CONFIG) as dagger_client:
        async with anyio.create_task_group() as tg:
            for context in contexts:
                context.dagger_client = dagger_client.pipeline(f"{context.connector.technical_name} - Test Pipeline")
                tg.start_soon(run_connector_test_pipeline, context, semaphore)


async def run_connector_build_pipeline(context: ConnectorTestContext, semaphore: anyio.Semaphore) -> ConnectorTestReport:
    """Run a build pipeline for a single connector.

    Args:
        context (ConnectorTestContext): The initialized connector test context.

    Returns:
        ConnectorTestReport: The test reports holding tests results.
    """
    async with semaphore:
        async with context:
            build_results_per_platform = await builds.run_connector_build(context)
            step_results = [value[0] for value in build_results_per_platform.values()]
            if context.is_local:
                _, connector_container = build_results_per_platform[builds.LOCAL_BUILD_PLATFORM]
                load_image_result = await builds.load_connector_container_to_local_docker_host(context, connector_container)
                step_results.append(load_image_result)
            context.test_report = ConnectorTestReport(context, step_results)
        return context.test_report


async def run_connectors_build_pipelines(contexts: List[ConnectorTestContext], concurrency: int = 5):
    """Run a build pipeline for all the connector contexts.

    Args:
        contexts (List[ConnectorTestContext]): List of connector contexts for which a build pipeline needs to be run.
        concurrency (int): Number of test pipeline that can run in parallel. Defaults to 5
    """
    semaphore = anyio.Semaphore(concurrency)
    async with dagger.Connection(DAGGER_CONFIG) as dagger_client:
        async with anyio.create_task_group() as tg:
            for context in contexts:
                context.dagger_client = dagger_client.pipeline(f"{context.connector.technical_name} - Build Pipeline")
                tg.start_soon(run_connector_build_pipeline, context, semaphore)
