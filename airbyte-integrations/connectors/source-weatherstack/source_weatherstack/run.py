#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_weatherstack import SourceWeatherstack


def run():
    source = SourceWeatherstack()
    launch(source, sys.argv[1:])
