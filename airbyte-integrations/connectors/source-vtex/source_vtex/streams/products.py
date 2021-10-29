import requests
from typing import Any, Mapping, Iterable, Optional, MutableMapping
from source_vtex.base_streams import VtexStream
from airbyte_cdk.sources.streams.http.http import HttpSubStream


class Products(HttpSubStream, VtexStream):
    """
    This stream brings the product data. It's mandatory to pass a product id,
    so it has to be a SubStream, therefore in this case a stream that brings
    product id should be set as parent for this Products stream.
    """

    primary_key = "productId"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        product_id = stream_slice["parent"][self.primary_key]
        return f"/api/catalog/pvt/product/{product_id}"

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        return {}

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        return None

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping]:
        yield response.json()
