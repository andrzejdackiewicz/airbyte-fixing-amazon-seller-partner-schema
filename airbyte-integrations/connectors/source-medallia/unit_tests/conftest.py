#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import pendulum
import pytest


@pytest.fixture()
def url_base():
    """
    URL base for test
    """
    return "https://test_domain.okta.com"


@pytest.fixture()
def api_url(url_base):
    """
    Just return API url based on url_base
    """
    return f"{url_base}"


@pytest.fixture()
def oauth_config():
    """
    Credentials for oauth2.0 authorization
    """
    return {
        "credentials": {
            "auth_type": "oauth2.0",
            "client_secret": "test_client_secret",
            "client_id": "test_client_id",
            "refresh_token": "test_refresh_token",
        },
        "domain": "test_domain",
    }

@pytest.fixture()
def wrong_oauth_config_bad_auth_type():
    """
    Wrong Credentials format for oauth2.0 authorization
    absent "auth_type" field
    """
    return {
        "credentials": {
            "client_secret": "test_client_secret",
            "client_id": "test_client_id",
            "refresh_token": "test_refresh_token",
        },
        "domain": "test_domain",
    }
