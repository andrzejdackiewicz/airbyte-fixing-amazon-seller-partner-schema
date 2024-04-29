#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_lever_hiring import SourceLeverHiring


def run():
    source = SourceLeverHiring()
    launch(source, sys.argv[1:])
