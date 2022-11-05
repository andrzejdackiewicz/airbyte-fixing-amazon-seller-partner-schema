#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from bigquery_schema_generator.generate_schema import SchemaGenerator
from gbqschema_converter.gbqschema_to_jsonschema import json_representation as converter
import json
import sys


from abc import ABC
from imaplib import _Authenticator
from pydoc import doc
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

from airbyte_cdk.models import SyncMode
from datetime import datetime, timedelta 
from  dateutil.parser import parse

import requests
import asyncio
import base64
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator, NoAuth


class SurveyStream(HttpStream, ABC):

    def __init__(self, config: Mapping[str, Any], form_id, **kwargs):
        super().__init__()
        self.server_name = config['server_name']
        self.form_id = form_id
        self.start_date = config['start_date']
        #base64 encode username and password as auth token
        user_name_password = f"{config['username']}:{config['password']}"
        self.auth_token = self._base64_encode(user_name_password)


    @property
    def url_base(self) -> str:
         return f"https://{self.server_name}.surveycto.com/api/v2/forms/data/wide/json/"

    def _base64_encode(self,string:str) -> str:
        return base64.b64encode(string.encode("ascii")).decode("ascii")

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        
        return {}

class SurveyctoStream(SurveyStream):

    primary_key = 'KEY'
    date_format = '%b %d, %Y %H:%M:%S %p'
    dateformat =  '%Y-%m-%dT%H:%M:%S'
    cursor_field = 'CompletionDate'
    _cursor_value = None

    @property
    def name(self) -> str:
        return self.form_id

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    # def _base64_encode(self,string:str) -> str:
    #     return base64.b64encode(string.encode("ascii")).decode("ascii")

    def get_json_schema(self):
        generator = SchemaGenerator(input_format='dict', infer_mode='NULLABLE',preserve_input_sort_order='true')

        data = self.response_json
        schema_map, error_logs = generator.deduce_schema(input_data=data)
        schema = generator.flatten_schema(schema_map)     
        schema_json = converter(schema)
   
        schema_json_properties=schema_json['definitions']['element']['properties']
        print(f'===--------=====================>{schema_json_properties}')


        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "additionalProperties": True,
            "type": "object",
            "properties": {'CompletionDate': {'type': 'string'}, 'SubmissionDate': {'type': 'string'}, 'starttime': {'type': 'string'}, 'endtime': {'type': 'string'}, 'deviceid': {'type': 'string'}, 'subscriberid': {'type': 'string'}, 'simid': {'type': 'string'}, 'devicephonenum': {'type': 'string'}, 'username': {'type': 'string'}, 'duration': {'type': 'integer'}, 'caseid': {'type': 'string'}, 'cdo_knows': {'type': 'integer'}, 'search_didi': {'type': 'string'}, 'unique_id_list': {'type': 'string'}, 'Selected_name_list': {'type': 'string'}, 'Selected_district_list': {'type': 'string'}, 'Selected_block_list': {'type': 'string'}, 'Selected_Panchayat_name_list': {'type': 'string'}, 'Selected_didi_village_list': {'type': 'string'}, 'farming_land': {'type': 'integer'}, 'ddlp_lowlandamt': {'type': 'integer'}, 'ddlp_midlandamt': {'type': 'number'}, 'ddlp_uplandamt': {'type': 'integer'}, 'farming_in_a_year': {'type': 'string'}, 'cuurent_cultivation_practised__________': {'type': 'integer'}, 'lease_season________': {'type': 'integer'}, 'irrigation': {'type': 'string'}, 'water_available_for_irrigation_apr': {'type': 'integer'}, 'water_available_for_irrigation_sep': {'type': 'integer'}, 'mgnrega_asset_creation': {'type': 'integer'}, 'ddlp_pumpset_yn': {'type': 'integer'}, 'pig': {'type': 'integer'}, 'goat': {'type': 'integer'}, 'Cow': {'type': 'integer'}, 'ox': {'type': 'integer'}, 'current_shedspace': {'type': 'integer'}, 'small_business': {'type': 'integer'}, 'ddlp_mentalmath_yn': {'type': 'integer'}, 'ddlp_agrplan_yn': {'type': 'integer'}, 'rabii': {'type': 'integer'}, 'rabi_count': {'type': 'integer'}, 'rabi_crop_count': {'type': 'integer'}, 'crop_type_1': {'type': 'string'}, 'land_amt_1': {'type': 'integer'}, 'crop_type_2': {'type': 'string'}, 'land_amt_2': {'type': 'integer'}, 'rabi_start_date': {'type': 'string'}, 'kharif': {'type': 'integer'}, 'kharif_numcrops': {'type': 'integer'}, 'kharif_crop_count': {'type': 'string'}, 'crop_type_kharif_1': {'type': 'string'}, 'land_amt_kharif_1': {'type': 'integer'}, 'kharif_start_date': {'type': 'string'}, 'possible_const_govtschemes_land_leveling': {'type': 'integer'}, 'land_papers': {'type': 'integer'}, 'farming_help': {'type': 'string'}, 'ddlp_livplan_yn': {'type': 'integer'}, 'ddlp_busplan_yn': {'type': 'integer'}, 'instanceID': {'type': 'string'}, 'formdef_version': {'type': 'integer'}, 'review_quality': {'type': 'string'}, 'review_status': {'type': 'string'}, 'KEY': {'type': 'string'}, 'cuurent_cultivation_practised______': {'type': 'integer'}, 'lease_season_None': {'type': 'integer'}, 'water_available_for_irrigation_aug': {'type': 'integer'}, 'water_available_for_irrigation_nov': {'type': 'integer'}, 'asset_type_Landdevelopment': {'type': 'integer'}, 'possible_const_govtschemes_land_improv': {'type': 'integer'}, 'ddlp_busselect': {'type': 'string'}, 'business_place_tola': {'type': 'integer'}, 'business_time': {'type': 'string'}, 'ddlp_bushelp_yn': {'type': 'integer'}, 'sb_date': {'type': 'string'}, 'dada_agreement': {'type': 'integer'}, 'select_Team': {'type': 'string'}, 'select_cdo': {'type': 'string'}, 'Village': {'type': 'string'}, 'select_hamlet': {'type': 'string'}, 'didi_name': {'type': 'string'}, 'unique_id': {'type': 'string'}, 'presence': {'type': 'string'}, 'dada_during_plan': {'type': 'integer'}, 'in_ex_gap': {'type': 'integer'}, 'phone_num_yn': {'type': 'integer'}, 'phone_num_neighbour': {'type': 'integer'}, 'cuurent_cultivation_practised____': {'type': 'integer'}, 'water_available_for_irrigation_oct': {'type': 'integer'}, 'asset_type_WHS': {'type': 'integer'}, 'crop_type_3': {'type': 'string'}, 'land_amt_3': {'type': 'integer'}, 'crop_type_4': {'type': 'string'}, 'land_amt_4': {'type': 'integer'}, 'possible_const_govtschemes_ya': {'type': 'integer'}, 'cuurent_cultivation_practised_______': {'type': 'integer'}, 'water_available_for_irrigation_jan': {'type': 'integer'}, 'water_available_for_irrigation_feb': {'type': 'integer'}, 'water_available_for_irrigation_mar': {'type': 'integer'}, 'lease_season_____': {'type': 'integer'}, 'lease_season_NA': {'type': 'integer'}, 'asset_type_DBI': {'type': 'integer'}, 'asset_type_Other': {'type': 'integer'}, 'Know': {'type': 'string'}},
        }

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
         return self.form_id

    @property
    def state(self) -> Mapping[str, Any]:
        initial_date = datetime.strptime(self.start_date, self.date_format)
        if self._cursor_value:
            return {self.cursor_field: self._cursor_value}
        else:
            return {self.cursor_field: initial_date}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = datetime.strptime(value[self.cursor_field], self.dateformat)

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
         ix = self.state[self.cursor_field] 
         return {'date': ix.strftime(self.date_format)}

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        return {'Authorization': 'Basic ' + self.auth_token }

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        self.response_json = response.json()
    
        for data in self.response_json:
            try:
                yield data
            except Exception as e:
                msg = f"""Encountered an exception parsing schema"""
                self.logger.exception(msg)
                raise e

# Source
class SourceSurveycto(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        return True, None

    def no_auth(self):
        return NoAuth()     

    def generate_streams(self, config: str) -> List[Stream]:
        forms = config.get("form_id", [])
        for form_id in forms:
            yield SurveyctoStream(
                config=config,
                form_id=form_id
            )

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        # auth = TokenAuthenticator(token="api_key")  # Oauth2Authenticator is also available if you need oauth support
        # return [Customers(authenticator=auth), Employees(authenticator=auth)]
        # auth = NoAuth()        

        streams = self.generate_streams(config=config)
        return streams


