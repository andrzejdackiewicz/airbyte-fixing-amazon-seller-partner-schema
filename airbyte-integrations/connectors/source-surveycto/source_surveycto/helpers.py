
from typing import Any, Dict

import requests
import base64
from airbyte_cdk.models import AirbyteStream
from airbyte_cdk.models.airbyte_protocol import DestinationSyncMode, SyncMode
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
from bigquery_schema_generator.generate_schema import SchemaGenerator
from gbqschema_converter.gbqschema_to_jsonschema import json_representation as converter
import json
import asyncio
import time


class Helpers(object):

    @staticmethod
    def _base64_encode(string:str) -> str:
        return base64.b64encode(string.encode("ascii")).decode("ascii")

    @staticmethod
    def call_survey_cto(config, form_id):
        time.sleep(3)
        print(f'{config}')
        print(f'{form_id}')
        server_name = config['server_name']
        print(f'=========================>>>>{server_name}')
        start_date = config['start_date']
        print(f'=========================>>>>{start_date}')
        user_name_password = f"{config['username']}:{config['password']}"
        print(f'=========================>>>>{user_name_password}')
        auth_token = Helpers._base64_encode(user_name_password)
        print(f'=========================>>>>{auth_token}')
        
        url = f"https://{server_name}.surveycto.com/api/v2/forms/data/wide/json/{form_id}?date={start_date}"
        
        response = requests.get(url, headers = {'Authorization': 'Basic ' + auth_token })
        print(f'=========================>>>>{response}')
        data = response.json()
        print(f'=========================>>>>{data}')
        generator = SchemaGenerator(input_format='dict', infer_mode='NULLABLE',preserve_input_sort_order='true')

        schema_map, error_logs = generator.deduce_schema(input_data=data)
        schema = generator.flatten_schema(schema_map)
        schema_json = converter(schema)
        schema_json_properties=schema_json['definitions']['element']['properties']
        schema = json.dumps(schema_json_properties)
        
        return schema