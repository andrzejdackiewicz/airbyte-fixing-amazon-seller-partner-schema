#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_breezometer_air_quality import SourceBreezometerAirQuality

if __name__ == "__main__":
    source = SourceBreezometerAirQuality()
    launch(source, sys.argv[1:])
