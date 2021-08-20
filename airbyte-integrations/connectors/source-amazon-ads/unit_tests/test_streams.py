#
# MIT License
#
# Copyright (c) 2020 Airbyte
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

import json
from http import HTTPStatus
from urllib.parse import parse_qs, urlparse

import pytest
import requests
import responses
from airbyte_cdk.models import SyncMode
from jsonschema import validate
from source_amazon_ads import SourceAmazonAds


def setup_responses(
    profiles_response=None,
    campaigns_response=None,
    adgroups_response=None,
    targeting_response=None,
    product_ads_response=None,
    generic_response=None,
):
    responses.add(
        responses.POST,
        "https://api.amazon.com/auth/o2/token",
        json={"access_token": "alala", "expires_in": 10},
    )
    if profiles_response:
        responses.add(
            responses.GET,
            "https://advertising-api.amazon.com/v2/profiles",
            body=profiles_response,
        )
    if campaigns_response:
        responses.add(
            responses.GET,
            "https://advertising-api.amazon.com/sd/campaigns",
            body=campaigns_response,
        )
    if adgroups_response:
        responses.add(
            responses.GET,
            "https://advertising-api.amazon.com/sd/adGroups",
            body=adgroups_response,
        )
    if targeting_response:
        responses.add(
            responses.GET,
            "https://advertising-api.amazon.com/sd/targets",
            body=targeting_response,
        )
    if product_ads_response:
        responses.add(
            responses.GET,
            "https://advertising-api.amazon.com/sd/productAds",
            body=product_ads_response,
        )
    if generic_response:
        responses.add(
            responses.GET,
            f"https://advertising-api.amazon.com/{generic_response}",
            json=[],
        )


def get_all_stream_records(stream):
    records = stream.read_records(SyncMode.full_refresh)
    return [r for r in records]


def get_stream_by_name(streams, stream_name):
    for stream in streams:
        if stream.name == stream_name:
            return stream
    raise Exception(f"Expected stream {stream_name} not found")


@responses.activate
def test_streams_profile(test_config, profiles_response):
    setup_responses(profiles_response=profiles_response)

    source = SourceAmazonAds()
    streams = source.streams(test_config)

    profile_stream = get_stream_by_name(streams, "profiles")
    schema = profile_stream.get_json_schema()
    records = get_all_stream_records(profile_stream)
    assert len(responses.calls) == 2
    assert len(profile_stream._profiles) == 4
    assert len(records) == 4
    expected_records = json.loads(profiles_response)
    for record, expected_record in zip(records, expected_records):
        validate(schema=schema, instance=record)
        assert record == expected_record


@responses.activate
def test_streams_campaigns_4_vendors(test_config, profiles_response, campaigns_response):
    profiles_response = json.loads(profiles_response)
    for profile in profiles_response:
        profile["accountInfo"]["type"] = "vendor"
    profiles_response = json.dumps(profiles_response)
    setup_responses(profiles_response=profiles_response, campaigns_response=campaigns_response)

    source = SourceAmazonAds()
    streams = source.streams(test_config)
    profile_stream = get_stream_by_name(streams, "profiles")
    campaigns_stream = get_stream_by_name(streams, "sponsored_display_campaigns")
    profile_records = get_all_stream_records(profile_stream)
    campaigns_records = get_all_stream_records(campaigns_stream)
    assert len(campaigns_records) == len(profile_records) * len(json.loads(campaigns_response))


@pytest.mark.parametrize(
    ("page_size"),
    [1, 2, 5, 1000000],
)
@responses.activate
def test_streams_campaigns_pagination(mocker, test_config, profiles_response, campaigns_response, page_size):
    mocker.patch("source_amazon_ads.streams.common.PaginationStream.page_size", page_size)
    profiles_response = json.loads(profiles_response)
    for profile in profiles_response:
        profile["accountInfo"]["type"] = "vendor"
    profiles_response = json.dumps(profiles_response)
    setup_responses(profiles_response=profiles_response)

    source = SourceAmazonAds()
    streams = source.streams(test_config)
    profile_stream = get_stream_by_name(streams, "profiles")
    campaigns_stream = get_stream_by_name(streams, "sponsored_display_campaigns")
    campaigns = json.loads(campaigns_response)

    def campaigns_paginated_response_cb(request):
        query = urlparse(request.url).query
        query = parse_qs(query)
        start_index, count = (int(query.get(f, [0])[0]) for f in ["startIndex", "count"])
        response_body = campaigns[start_index : start_index + count]
        return (200, {}, json.dumps(response_body))

    responses.add_callback(
        responses.GET,
        "https://advertising-api.amazon.com/sd/campaigns",
        content_type="application/json",
        callback=campaigns_paginated_response_cb,
    )
    profile_records = get_all_stream_records(profile_stream)

    campaigns_records = get_all_stream_records(campaigns_stream)
    assert len(campaigns_records) == len(profile_records) * len(json.loads(campaigns_response))


@pytest.mark.parametrize(("status_code"), [HTTPStatus.FORBIDDEN, HTTPStatus.UNAUTHORIZED])
@responses.activate
def test_streams_campaigns_pagination_403_error(mocker, status_code, test_config, profiles_response, campaigns_response):
    setup_responses(profiles_response=profiles_response)
    responses.add(
        responses.GET,
        "https://advertising-api.amazon.com/sd/campaigns",
        json={"message": "msg"},
        status=status_code,
    )
    source = SourceAmazonAds()
    streams = source.streams(test_config)
    campaigns_stream = get_stream_by_name(streams, "sponsored_display_campaigns")

    with pytest.raises(requests.exceptions.HTTPError):
        get_all_stream_records(campaigns_stream)


@responses.activate
def test_streams_campaigns_pagination_403_error_expected(mocker, test_config, profiles_response, campaigns_response):
    setup_responses(profiles_response=profiles_response)
    responses.add(
        responses.GET,
        "https://advertising-api.amazon.com/sd/campaigns",
        json={"code": "403", "details": "details", "requestId": "xxx"},
        status=403,
    )
    source = SourceAmazonAds()
    streams = source.streams(test_config)
    campaigns_stream = get_stream_by_name(streams, "sponsored_display_campaigns")

    campaigns_records = get_all_stream_records(campaigns_stream)
    assert campaigns_records == []


@pytest.mark.parametrize(
    ("stream_name", "endpoint"),
    [
        ("sponsored_display_ad_groups", "sd/adGroups"),
        ("sponsored_display_product_ads", "sd/productAds"),
        ("sponsored_display_targetings", "sd/targets"),
    ],
)
@responses.activate
def test_streams_displays(
    test_config,
    stream_name,
    endpoint,
    profiles_response,
    adgroups_response,
    targeting_response,
    product_ads_response,
):
    setup_responses(
        profiles_response=profiles_response,
        adgroups_response=adgroups_response,
        targeting_response=targeting_response,
        product_ads_response=product_ads_response,
    )

    source = SourceAmazonAds()
    streams = source.streams(test_config)
    test_stream = get_stream_by_name(streams, stream_name)

    records = get_all_stream_records(test_stream)
    assert len(records) == 4
    schema = test_stream.get_json_schema()
    for r in records:
        validate(schema=schema, instance=r)
    assert any([endpoint in call.request.url for call in responses.calls])


@pytest.mark.parametrize(
    ("stream_name", "endpoint"),
    [
        ("sponsored_brands_campaigns", "sb/campaigns"),
        ("sponsored_brands_ad_groups", "sb/adGroups"),
        ("sponsored_brands_keywords", "sb/keywords"),
        ("sponsored_product_campaigns", "v2/sp/campaigns"),
        ("sponsored_product_ad_groups", "v2/sp/adGroups"),
        ("sponsored_product_keywords", "v2/sp/keywords"),
        ("sponsored_product_negative_keywords", "v2/sp/negativeKeywords"),
        ("sponsored_product_ads", "v2/sp/productAds"),
        ("sponsored_product_targetings", "v2/sp/targets"),
    ],
)
@responses.activate
def test_streams_brands_and_products(test_config, stream_name, endpoint, profiles_response):
    setup_responses(profiles_response=profiles_response, generic_response=endpoint)

    source = SourceAmazonAds()
    streams = source.streams(test_config)
    test_stream = get_stream_by_name(streams, stream_name)

    records = get_all_stream_records(test_stream)
    assert records == []
    assert any([endpoint in call.request.url for call in responses.calls])
