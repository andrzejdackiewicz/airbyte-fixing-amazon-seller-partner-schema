from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from urllib import request

import requests

from .basic_streams import AppleSearchAdsStream

class WithCampaignAppleSearchAdsStream(AppleSearchAdsStream, ABC):
    cursor_field = "date"

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        return {}

    def _chunk_campaigns_range(self) -> List[Mapping[str, any]]:
        response = requests.request(
                "GET",
                url=f"{self.url_base}campaigns",
                headers={
                    "X-AP-Context": f"orgId={self.org_id}",
                    **self.authenticator.get_auth_header()
                },
                params={
                    "limit": self.limit
                }
            )

        campaign_ids = []

        for campaign in response.json()["data"]:
            campaign_ids.append({
                "campaign_id": campaign["id"],
                "adam_id": campaign["adamId"]
            })

        return campaign_ids

    def stream_slices(self, sync_mode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None) -> Iterable[
        Optional[Mapping[str, any]]]:

        return self._chunk_campaigns_range()

class Adgroups(WithCampaignAppleSearchAdsStream):
    primary_key = ["id"]

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"campaigns/{stream_slice.get('campaign_id')}/adgroups"

class CampaignNegativeKeywords(WithCampaignAppleSearchAdsStream):
    primary_key = ["id"]

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"campaigns/{stream_slice.get('campaign_id')}/adgroups"

class CreativeSets(WithCampaignAppleSearchAdsStream):
    primary_key = ["id"]

    @property
    def http_method(self) -> str:
        return "POST"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"creativesets/find"

    def request_body_json(
        self, stream_slice: Mapping[str, Any] = None, **kwargs: Any
    ) -> Optional[Mapping]:
        post_json = {
            "selector": {
                "conditions": [
                    {
                        "field": "adamId",
                        "operator": "EQUALS",
                        "values": [
                            stream_slice.get('adam_id')
                        ]
                    }
                ]
            }
        }

        return post_json
