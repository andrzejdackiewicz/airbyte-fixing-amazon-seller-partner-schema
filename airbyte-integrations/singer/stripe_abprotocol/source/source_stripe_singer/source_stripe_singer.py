from airbyte_protocol import Source
from airbyte_protocol import AirbyteSpec
from airbyte_protocol import AirbyteCheckResponse
from airbyte_protocol import AirbyteCatalog
from airbyte_protocol import AirbyteMessage
import requests
from typing import Generator
from base_singer import SingerHelper


class SourceStripeSinger(Source):
    def __init__(self):
        pass

    def spec(self) -> AirbyteSpec:
        return SingerHelper.spec_from_file('/airbyte/stripe-files/spec.json')

    def check(self, logger, config_container) -> AirbyteCheckResponse:
        json_config = config_container.rendered_config
        r = requests.get('https://api.stripe.com/v1/customers', auth=(json_config['client_secret'], ''))

        return AirbyteCheckResponse(r.status_code == 200, {})

    def discover(self, logger, config_container) -> AirbyteCatalog:
        catalogs = SingerHelper.get_catalogs(logger, f"tap-stripe --config {config_container.rendered_config_path} --discover")
        return catalogs.airbyte_catalog

    def read(self, logger, config_container, catalog_path, state=None) -> Generator[AirbyteMessage, None, None]:
        masked_airbyte_catalog = self.read_config(catalog_path)
        discovered_singer_catalog = SingerHelper.get_catalogs(logger, f"tap-stripe --config {config_container.rendered_config_path} --discover").singer_catalog
        combined_catalog_path = SingerHelper.combine_catalogs(masked_airbyte_catalog, discovered_singer_catalog)

        if state:
            return SingerHelper.read(logger, f"tap-stripe --config {config_container.rendered_config_path} --catalog {combined_catalog_path} --state {state}")
        else:
            return SingerHelper.read(logger, f"tap-stripe --config {config_container.rendered_config_path} --catalog {combined_catalog_path}")
