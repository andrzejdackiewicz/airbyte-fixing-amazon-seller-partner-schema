from source_dv_360.source import SourceDV360
from  google.oauth2.credentials import Credentials


def test_streams_count(config):
    source = SourceDV360()
    streams = source.streams(config)
    expected_streams_number = 5
    assert len(streams) == expected_streams_number


SAMPLE_CONFIG = {
  "credentials": {
    "access_token": "access_token",
    "refresh_token": "refresh_token",
    "token_uri": "uri",
    "client_id": "client_id",
    "client_secret": "client_secret"
  },
  "start_date": "2022-03-01",
  "end_date": "2022-03-08",
  "partner_id": 123,
  "filters": []
}


EXPECTED_CRED = {
    "access_token": "access_token",
    "refresh_token": "refresh_token",
    "token_uri": "uri",
    "client_id": "client_id",
    "client_secret": "client_secret"
}


def test_get_credentials():
    client = SourceDV360()
    credentials = client.get_credentials(SAMPLE_CONFIG)

    assert credentials.token == 'access_token'
    assert credentials.refresh_token == 'refresh_token'
    assert credentials.token_uri == 'uri'
    assert credentials.client_id == 'client_id'
    assert credentials.client_secret == 'client_secret'