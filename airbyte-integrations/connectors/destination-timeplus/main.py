#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from destination_timeplus import DestinationTimeplus

if __name__ == "__main__":
    DestinationTimeplus().run(sys.argv[1:])
