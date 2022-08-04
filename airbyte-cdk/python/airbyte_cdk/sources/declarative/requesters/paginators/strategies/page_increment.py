#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from dataclasses import dataclass
from typing import Any, List, Mapping, Optional

import requests
from airbyte_cdk.sources.declarative.requesters.paginators.strategies.pagination_strategy import PaginationStrategy
from dataclasses_jsonschema import JsonSchemaMixin


@dataclass
class PageIncrement(PaginationStrategy, JsonSchemaMixin):
    """
    Pagination strategy that returns the number of pages reads so far and returns it as the next page token

    Attributes:
        page_size (int): the number of records to request
    """

    page_size: int

    def __post_init__(self):
        self._offset = 0

    def next_page_token(self, response: requests.Response, last_records: List[Mapping[str, Any]]) -> Optional[Any]:
        if len(last_records) < self.page_size:
            return None
        else:
            self._offset += 1
            return self._offset
