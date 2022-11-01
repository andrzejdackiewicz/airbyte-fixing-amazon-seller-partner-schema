#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_younium import SourceYounium

if __name__ == "__main__":
    source = SourceYounium()
    launch(source, sys.argv[1:])
