#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

import pytest


@pytest.fixture
def mock_api_client(mocker):
    return mocker.Mock()
