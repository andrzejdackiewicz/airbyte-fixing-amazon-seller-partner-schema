#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from source_drift import SourceDrift

from airbyte_cdk.entrypoint import launch

if __name__ == "__main__":
    source = SourceDrift()
    launch(source, sys.argv[1:])
