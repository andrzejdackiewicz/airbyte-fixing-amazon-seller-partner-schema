#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_bamboo_hr import SourceBambooHr


def run():
    source = SourceBambooHr()
    launch(source, sys.argv[1:])
