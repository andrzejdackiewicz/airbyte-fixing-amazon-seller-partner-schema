#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import sys
import traceback
from datetime import datetime

from source_azure_blob_storage import Config, SourceAzureBlobStorage, SourceAzureBlobStorageStreamReader

from airbyte_cdk.entrypoint import AirbyteEntrypoint, launch
from airbyte_cdk.models import AirbyteErrorTraceMessage, AirbyteMessage, AirbyteTraceMessage, TraceType, Type


if __name__ == "__main__":
    args = sys.argv[1:]
    catalog_path = AirbyteEntrypoint.extract_catalog(args)
    source = SourceAzureBlobStorage(SourceAzureBlobStorageStreamReader(), Config, catalog_path)
    try:
        launch(source, args)
    except KeyError:
        print(
            AirbyteMessage(
                type=Type.TRACE,
                trace=AirbyteTraceMessage(
                    type=TraceType.ERROR,
                    emitted_at=int(datetime.now().timestamp() * 1000),
                    error=AirbyteErrorTraceMessage(
                        message="Error starting the sync. This could be due to an invalid configuration or catalog. Please contact Support for assistance.",
                        stack_trace=traceback.format_exc(),
                    ),
                ),
            ).json()
        )
