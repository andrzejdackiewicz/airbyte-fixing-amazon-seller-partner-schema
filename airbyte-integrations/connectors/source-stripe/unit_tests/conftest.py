#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import pytest
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator


@pytest.fixture(autouse=True)
def disable_cache(mocker):
    for cls in [
        "ApplicationFees", "Invoices", "CheckoutSessions", "Customers", "Accounts", "Transfers", "Subscriptions", "SubscriptionItems"
    ]:
        mocker.patch(
            f"source_stripe.streams.{cls}.use_cache",
            new_callable=mocker.PropertyMock,
            return_value=False
        )


@pytest.fixture(name="config")
def config_fixture():
    config = {"client_secret": "sk_test(live)_<secret>",
              "account_id": "<account_id>", "start_date": "2020-05-01T00:00:00Z"}
    return config


@pytest.fixture(name="stream_args")
def stream_args_fixture():
    authenticator = TokenAuthenticator("sk_test(live)_<secret>")
    args = {
        "authenticator": authenticator,
        "account_id": "<account_id>",
        "start_date": 1588315041,
        "slice_range": 365,
    }
    return args
