#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_sftp_bulk import SourceFtp


def run():
    source = SourceFtp()
    launch(source, sys.argv[1:])
