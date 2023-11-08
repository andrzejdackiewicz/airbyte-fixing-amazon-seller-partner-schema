#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import pytest
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from source_stripe.source import SourceStripe


@pytest.fixture(name="config")
def config_fixture():
    config = {"client_secret": "sk_test(live)_<secret>", "account_id": "<account_id>", "start_date": "2020-05-01T00:00:00Z"}
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


@pytest.fixture(name="incremental_stream_args")
def incremental_args_fixture(stream_args):
    return {"lookback_window_days": 14, **stream_args}


@pytest.fixture()
def stream_by_name(config):
    def mocker(stream_name, source_config=config):
        source = SourceStripe()
        streams = source.streams(source_config)
        for stream in streams:
            if stream.name == stream_name:
                return stream

    return mocker
