#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from typing import TYPE_CHECKING, Any, List, Mapping, Tuple

from airbyte_cdk.sources import AbstractSource

from .sheet import SmartSheetAPIWrapper
from .streams import SmartsheetStream

if TYPE_CHECKING:
    from airbyte_cdk.sources.streams import Stream


class SourceSmartsheets(AbstractSource):
    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, any]:
        sheet = SmartSheetAPIWrapper(config)
        return sheet.check_connection(logger)

    def streams(self, config: Mapping[str, Any]) -> List["Stream"]:
        sheet = SmartSheetAPIWrapper(config)
        return [SmartsheetStream(sheet, config)]
