#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

import typing as t

import click


class OctaviaCommand(click.Command):
    def make_context(
        self, info_name: t.Optional[str], args: t.List[str], parent: t.Optional[click.Context] = None, **extra: t.Any
    ) -> click.Context:
        """Wrap parent make context with telemetry sending in case of failure.

        Args:
            info_name (t.Optional[str]): The info name for this invocation.
            args (t.List[str]): The arguments to parse as list of strings.
            parent (t.Optional[click.Context], optional): The parent context if available.. Defaults to None.

        Raises:
            e: Raise whatever exception that was catch.

        Returns:
            click.Context: The built context.
        """
        try:
            return super().make_context(info_name, args, parent, **extra)
        except Exception as e:
            telemetry_client = parent.obj["TELEMETRY_CLIENT"]
            telemetry_client.send_command_telemetry(parent, error=e, extra_info_name=info_name)
            raise e

    def invoke(self, ctx: click.Context) -> t.Any:
        """Wrap parent invoke by sending telemetry in case of success or failure.

        Args:
            ctx (click.Context): The invocation context.

        Raises:
            e: Raise whatever exception that was catch.

        Returns:
            t.Any: The invocation return value.
        """
        telemetry_client = ctx.obj["TELEMETRY_CLIENT"]
        try:
            result = super().invoke(ctx)
        except Exception as e:
            telemetry_client.send_command_telemetry(ctx, error=e)
            raise e
        telemetry_client.send_command_telemetry(ctx)
        return result
