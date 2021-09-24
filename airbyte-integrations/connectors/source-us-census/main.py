#
# Copyright (c) 2020 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_us_census import SourceUsCensus

if __name__ == "__main__":
    source = SourceUsCensus()
    launch(source, sys.argv[1:])
