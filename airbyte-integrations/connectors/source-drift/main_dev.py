#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


import sys

from base_python.entrypoint import launch
from source_drift import SourceDrift

if __name__ == "__main__":
    source = SourceDrift()
    launch(source, sys.argv[1:])
