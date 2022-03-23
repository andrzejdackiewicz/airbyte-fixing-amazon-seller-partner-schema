#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#
import json

from google.ads.googleads.errors import GoogleAdsException
from google.ads.googleads.v8 import GoogleAdsFailure


class MockSearchRequest:
    customer_id = "12345"
    query = None
    page_size = 100
    page_token = None


# Mocking Classes
class MockGoogleAdsService:
    def search(self, search_request):
        return search_request


class MockGoogleAdsClient:
    def __init__(self, config):
        self.config = config

    def get_type(self, type):
        return MockSearchRequest()

    def get_service(self, service):
        return MockGoogleAdsService()

    @staticmethod
    def load_from_dict(config):
        return MockGoogleAdsClient(config)


class MockErroringGoogleAdsService:
    def search(self, search_request):
        raise make_google_ads_exception(1)


def make_google_ads_exception(failure_code: int):
    # There is no easy way I could find to mock a GoogleAdsException without doing something heinous like this
    # Following the definition of the object here
    # https://developers.google.com/google-ads/api/reference/rpc/v10/GoogleAdsFailure
    protobuf_as_json = json.dumps({"errors": [{"error_code": {"request_error": failure_code}, "message": "it failed"}], "request_id": "1"})
    failure = type(GoogleAdsFailure).from_json(GoogleAdsFailure, protobuf_as_json)
    return GoogleAdsException(None, None, failure, 1)


class MockErroringGoogleAdsClient:
    def __init__(self, credentials, **kwargs):
        self.config = credentials
        self.customer_ids = ["1"]

    def send_request(self, query, customer_id):
        raise make_google_ads_exception(1)

    def get_type(self, type):
        return MockSearchRequest()

    def get_service(self, service):
        return MockErroringGoogleAdsService()

    @staticmethod
    def load_from_dict(config):
        return MockErroringGoogleAdsClient(config)
