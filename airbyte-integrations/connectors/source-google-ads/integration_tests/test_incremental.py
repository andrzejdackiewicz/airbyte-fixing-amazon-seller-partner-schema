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

import pendulum
from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import ConfiguredAirbyteCatalog, Type

from source_google_ads.source import SourceGoogleAds

SAMPLE_CATALOG = {
    "streams": [
        {
            "stream": {
                "name": "ad_group_ad_report",
                "json_schema": {
                    "type": "object",
                    "title": "Ad Group Ad Report",
                    "description": "An ad group ad.",
                    "properties": {
                        "accent_color": {
                            "description": "AccentColor",
                            "type": ["null", "string"],
                            "field": "ad_group_ad.ad.legacy_responsive_display_ad.accent_color",
                        },
                        "account_currency_code": {
                            "description": "AccountCurrencyCode",
                            "type": ["null", "string"],
                            "field": "customer.currency_code",
                        },
                        "account_descriptive_name": {
                            "description": "AccountDescriptiveName",
                            "type": ["null", "string"],
                            "field": "customer.descriptive_name",
                        },
                        "segments.date": {"description": "Date", "type": ["null", "string"], "field": "segments.date"},
                    },
                },
                "supported_sync_modes": ["full_refresh", "incremental"],
                "source_defined_cursor": True,
                "default_cursor_field": ["segments.date"],
            },
            "sync_mode": "incremental",
            "destination_sync_mode": "overwrite",
            "cursor_field": ["segments.date"],
        }
    ]
}

def test_incremental_sync():
    google_ads_client = SourceGoogleAds()
    state = "2021-05-24"
    records = google_ads_client.read(AirbyteLogger(), SAMPLE_CONFIG, ConfiguredAirbyteCatalog.parse_obj(SAMPLE_CATALOG), {"ad_group_ad_report": {
        "date": state
    }})
    current_state = pendulum.parse(state).subtract(days=14).to_date_string()

def test_incremental_sync(config):
    google_ads_client = SourceGoogleAds()
    state = "2021-05-24"
    records = google_ads_client.read(
        AirbyteLogger(), config, ConfiguredAirbyteCatalog.parse_obj(SAMPLE_CATALOG), {"ad_group_ad_report": {"segments.date": state}}
    )
    current_state = pendulum.parse(state).subtract(days=14).to_date_string()

    for record in records:
        if record and record.type == Type.STATE:
            print(record)
            current_state = record.state.data["ad_group_ad_report"]["segments.date"]
        if record and record.type == Type.RECORD:
            assert record.record.data["segments.date"] >= current_state

    # Next sync
    state = "2021-06-04"
    records = google_ads_client.read(
        AirbyteLogger(), config, ConfiguredAirbyteCatalog.parse_obj(SAMPLE_CATALOG), {"ad_group_ad_report": {"segments.date": state}}
    )
    current_state = pendulum.parse(state).subtract(days=14).to_date_string()

    for record in records:
        if record and record.type == Type.STATE:
            current_state = record.state.data["ad_group_ad_report"]["segments.date"]
        if record and record.type == Type.RECORD:
            assert record.record.data["segments.date"] >= current_state

    # Abnormal state
    state = "2029-06-04"
    records = google_ads_client.read(
        AirbyteLogger(), config, ConfiguredAirbyteCatalog.parse_obj(SAMPLE_CATALOG), {"ad_group_ad_report": {"segments.date": state}}
    )
    current_state = pendulum.parse(state).subtract(days=14).to_date_string()

    no_records = True
    for record in records:
        if record and record.type == Type.STATE:
            assert record.state.data["ad_group_ad_report"]["segments.date"] == state
        if record and record.type == Type.RECORD:
            no_records = False

    assert no_records
