#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from .source import SourcePingdom

def run():
    source = SourcePingdom()
    launch(source, sys.argv[1:])
