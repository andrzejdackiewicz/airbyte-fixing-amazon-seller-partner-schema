#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_coinmarketcap import SourceCoinmarketcap


def run():
    source = SourceCoinmarketcap()
    launch(source, sys.argv[1:])
