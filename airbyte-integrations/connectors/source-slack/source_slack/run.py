#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_slack import SourceSlack


def run():
    source = SourceSlack()
    launch(source, sys.argv[1:])
