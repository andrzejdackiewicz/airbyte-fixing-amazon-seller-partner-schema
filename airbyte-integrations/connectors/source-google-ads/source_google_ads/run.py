#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys
import traceback
from datetime import datetime
from typing import List

from airbyte_cdk.entrypoint import AirbyteEntrypoint, launch
from airbyte_cdk.models import AirbyteErrorTraceMessage, AirbyteMessage, AirbyteTraceMessage, TraceType, Type
from source_google_ads import SourceGoogleAds


def _get_source(args: List[str]):
    catalog_path = AirbyteEntrypoint.extract_catalog(args)
    config_path = AirbyteEntrypoint.extract_config(args)
    state_path = AirbyteEntrypoint.extract_state(args)
    try:
        return SourceGoogleAds(
            SourceGoogleAds.read_catalog(catalog_path) if catalog_path else None,
            SourceGoogleAds.read_config(config_path) if config_path else None,
            SourceGoogleAds.read_state(state_path) if state_path else None,
        )
    except Exception as error:
        print(
            AirbyteMessage(
                type=Type.TRACE,
                trace=AirbyteTraceMessage(
                    type=TraceType.ERROR,
                    emitted_at=int(datetime.now().timestamp() * 1000),
                    error=AirbyteErrorTraceMessage(
                        message=f"Error starting the sync. This could be due to an invalid configuration or catalog. Please contact Support for assistance. Error: {error}",
                        stack_trace=traceback.format_exc(),
                    ),
                ),
            ).json()
        )
        return None


def run():
    _args = sys.argv[1:]
    source = _get_source(_args)
    if source:
        launch(source, _args)
