#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import asyncclick as click
from pipelines.airbyte_ci.connectors.helpers.command import run_connector_pipeline
from pipelines.airbyte_ci.connectors.migrate_to_inline_schemas.pipeline import run_connector_migrate_to_inline_schemas_pipeline
from pipelines.cli.dagger_pipeline_command import DaggerPipelineCommand


@click.command(
    cls=DaggerPipelineCommand,
    short_help="Where possible (have a metadata.yaml), move stream schemas to inline schemas.",
)
@click.pass_context
async def migrate_to_inline_schemas(ctx: click.Context) -> bool:
    return await run_connector_pipeline(
        ctx, "Migrate to inline schemas", run_connector_migrate_to_inline_schemas_pipeline, enable_report_auto_open=False
    )
