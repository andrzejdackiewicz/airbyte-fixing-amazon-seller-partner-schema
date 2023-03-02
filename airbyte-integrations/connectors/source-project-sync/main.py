#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_project_sync import SourceProjectSync

if __name__ == "__main__":
    source = SourceProjectSync()
    launch(source, sys.argv[1:])
