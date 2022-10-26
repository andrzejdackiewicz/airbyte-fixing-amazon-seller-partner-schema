#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import json
import requests
from datetime import datetime
from typing import Dict, Generator

from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    Status,
    Type,
)
from airbyte_cdk.sources import Source

DECIDIM_GRAPHQL_URL = "decidim_graphql_url"

class DecidimStream(HttpStream, ABC):
    def __init__(self, decidim_graphql_url: str, **kwargs):
        super().__init__(**kwargs)
        self.decidim_graphql_url =  decidim_graphql_url
    
    primary_key = "id"
    response_entity = None
    
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        # The API does not offer pagination, so we return None to indicate there are no more pages in the response
        return None

    def path(
        self, 
        stream_state: Mapping[str, Any] = None, 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return ""  # TODO

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        return None  # TODO

    

class SourceDecidim(Source):
    def check(self, logger: AirbyteLogger, config: json) -> AirbyteConnectionStatus:
        """
        Tests if the input configuration can be used to successfully connect to the integration
            e.g: if a provided Stripe API token can be used to connect to the Stripe API.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
        the properties of the spec.yaml file

        :return: AirbyteConnectionStatus indicating a Success or Failure
        """
        try:
            import pdb; pdb.set_trace() 
            response = requests.post(
                config[DECIDIM_GRAPHQL_URL],
                json={"query":"{decidim{version}}"}
            )
            data = response.json()['data']
            
            if response.status_code == 200 and data['decidim']['version']:
                return AirbyteConnectionStatus(status=Status.SUCCEEDED)
            else:
                return AirbyteConnectionStatus(status=Status.FAILED, message=f"Provided URL of Decidim instance isn't correct. Please recheck decidim graphql URL.")
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {str(e)}")

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        # NoAuth just means there is no authentication required for this API and is included for completeness.
        # Skip passing an authenticator if no authentication is required.
        # Other authenticators are available for API token-based auth and Oauth2. 
        auth = NoAuth()  
        return [DecidimStream(config[DECIDIM_GRAPHQL_URL])]

    def discover(self, logger: AirbyteLogger, config: json) -> AirbyteCatalog:
        """
        Returns an AirbyteCatalog representing the available streams and fields in this integration.
        For example, given valid credentials to a Postgres database,
        returns an Airbyte catalog where each postgres table is a stream, and each table column is a field.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
        the properties of the spec.yaml file

        :return: AirbyteCatalog is an object describing a list of all available streams in this source.
            A stream is an AirbyteStream object that includes:
            - its stream name (or table name in the case of Postgres)
            - json_schema providing the specifications of expected schema for this stream (a list of columns described
            by their names and types)
        """
        streams = []

        stream_name = "TableName"  # Example
        json_schema = {  # Example
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {"columnName": {"type": "string"}},
        }

        # Not Implemented

        streams.append(AirbyteStream(name=stream_name, json_schema=json_schema))
        return AirbyteCatalog(streams=streams)

    def read(
        self, logger: AirbyteLogger, config: json, catalog: ConfiguredAirbyteCatalog, state: Dict[str, any]
    ) -> Generator[AirbyteMessage, None, None]:
        """
        Returns a generator of the AirbyteMessages generated by reading the source with the given configuration,
        catalog, and state.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
            the properties of the spec.yaml file
        :param catalog: The input catalog is a ConfiguredAirbyteCatalog which is almost the same as AirbyteCatalog
            returned by discover(), but
        in addition, it's been configured in the UI! For each particular stream and field, there may have been provided
        with extra modifications such as: filtering streams and/or columns out, renaming some entities, etc
        :param state: When a Airbyte reads data from a source, it might need to keep a checkpoint cursor to resume
            replication in the future from that saved checkpoint.
            This is the object that is provided with state from previous runs and avoid replicating the entire set of
            data everytime.

        :return: A generator that produces a stream of AirbyteRecordMessage contained in AirbyteMessage object.
        """
        stream_name = "TableName"  # Example
        data = {"columnName": "Hello World"}  # Example

        # Not Implemented

        yield AirbyteMessage(
            type=Type.RECORD,
            record=AirbyteRecordMessage(stream=stream_name, data=data, emitted_at=int(datetime.now().timestamp()) * 1000),
        )
