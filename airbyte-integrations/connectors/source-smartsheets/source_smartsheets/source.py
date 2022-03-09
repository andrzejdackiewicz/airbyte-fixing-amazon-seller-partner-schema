#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


from curses import meta
import json
from datetime import datetime
from typing import Any, Dict, Generator
from xml.etree.ElementInclude import include

import smartsheet
from airbyte_cdk import AirbyteLogger
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

# helpers
from airbyte_cdk.sources import Source


def get_prop(col_type: str) -> Dict[str, any]:
    props = {
        "TEXT_NUMBER": {"type": "string"},
        "DATE": {"type": "string", "format": "date"},
        "DATETIME": {"type": "string", "format": "date-time"},
    }
    return props.get(col_type, {"type": "string"})


def get_json_schema(sheet: Dict, include_metadata: bool) -> Dict:
    column_info = {i["title"]: get_prop(i["type"]) for i in sheet["columns"]}
    
    if include_metadata:
        metadata_fields = [i for i in sheet["rows"][0].keys() if i != 'cells']
        # assume string for metadata fields for now
        metadata_schema = {i: get_prop(i) for i in metadata_fields} 
        column_info.update(metadata_schema)
    
    json_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": column_info,
    }
    return json_schema

def catch(d: dict) -> Any:
    if 'value' in d:
        return d['value']
    return ''


# main class definition
class SourceSmartsheets(Source):
    def check(self, logger: AirbyteLogger, config: json) -> AirbyteConnectionStatus:
        try:
            access_token = config["access_token"]
            spreadsheet_id = config["spreadsheet_id"]

            smartsheet_client = smartsheet.Smartsheet(access_token)
            smartsheet_client.errors_as_exceptions(True)
            smartsheet_client.Sheets.get_sheet(spreadsheet_id)

            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            if isinstance(e, smartsheet.exceptions.ApiError):
                err = e.error.result
                code = 404 if err.code == 1006 else err.code
                reason = f"{err.name}: {code} - {err.message} | Check your spreadsheet ID."
            else:
                reason = str(e)
            logger.error(reason)
        return AirbyteConnectionStatus(status=Status.FAILED)

    def discover(self, logger: AirbyteLogger, config: json) -> AirbyteCatalog:
        access_token = config["access_token"]
        spreadsheet_id = config["spreadsheet_id"]
        include_metadata = config["include_metadata"]

        streams = []

        smartsheet_client = smartsheet.Smartsheet(access_token)
        try:
            sheet = smartsheet_client.Sheets.get_sheet(spreadsheet_id)
            sheet = json.loads(str(sheet))  # make it subscriptable
            sheet_json_schema = get_json_schema(sheet, include_metadata=include_metadata)

            logger.info(f"Running discovery on sheet: {sheet['name']} with {spreadsheet_id}")

            stream = AirbyteStream(name=sheet["name"], json_schema=sheet_json_schema)
            stream.supported_sync_modes = ["full_refresh"]
            streams.append(stream)

        except Exception as e:
            raise Exception(f"Could not run discovery: {str(e)}")

        return AirbyteCatalog(streams=streams)

    def read(
        self, logger: AirbyteLogger, config: json, catalog: ConfiguredAirbyteCatalog, state: Dict[str, any]
    ) -> Generator[AirbyteMessage, None, None]:

        access_token = config["access_token"]
        spreadsheet_id = config["spreadsheet_id"]
        include_metadata = config["include_metadata"]
        
        smartsheet_client = smartsheet.Smartsheet(access_token)

        for configured_stream in catalog.streams:
            stream = configured_stream.stream
            properties = stream.json_schema["properties"]
            if isinstance(properties, list):
                columns = tuple(key for dct in properties for key in dct.keys())
            elif isinstance(properties, dict):
                columns = tuple(i for i in properties.keys())
            else:
                logger.error("Could not read properties from the JSONschema in this stream")
            name = stream.name

            try:
                sheet = smartsheet_client.Sheets.get_sheet(spreadsheet_id)
                sheet = json.loads(str(sheet))  # make it subscriptable
                logger.info(f"Starting syncing spreadsheet {sheet['name']}")
                logger.info(f"Row count: {sheet['totalRowCount']}")

                for row in sheet["rows"]:
                    # convert all data to string as it is only expected format in schema
                    try:
                        id_name_map = {d['id']: d['title'] for d in sheet['columns']}
                        data = {id_name_map[i['columnId']]: catch(i) for i in row['cells']}

                        if include_metadata:
                            metadata = {i: row[i] for i in row.keys() if i != 'cells'}
                            data.update(metadata)
                            
                        yield AirbyteMessage(
                            type=Type.RECORD,
                            record=AirbyteRecordMessage(stream=name, data=data, emitted_at=int(datetime.now().timestamp()) * 1000),
                        )
                    except Exception as e:
                        logger.error(f"Unable to encode row into an AirbyteMessage with the following error: {e}")
                        raise

            except Exception as e:
                logger.error(f"Could not read smartsheet: {name}")
                raise e
        logger.info(f"Finished syncing spreadsheet with ID: {spreadsheet_id}")
