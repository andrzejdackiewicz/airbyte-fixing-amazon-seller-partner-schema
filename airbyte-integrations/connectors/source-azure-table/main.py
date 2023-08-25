#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from source_azure_table import SourceAzureTable

from airbyte_cdk.entrypoint import launch

if __name__ == "__main__":
    source = SourceAzureTable()
    launch(source, sys.argv[1:])
