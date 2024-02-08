#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_zuora import SourceZuora


def run():
    source = SourceZuora()
    launch(source, sys.argv[1:])
