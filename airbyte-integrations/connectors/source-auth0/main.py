#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from source_auth0 import SourceAuth0

from airbyte_cdk.entrypoint import launch

if __name__ == "__main__":
    source = SourceAuth0()
    launch(source, sys.argv[1:])
