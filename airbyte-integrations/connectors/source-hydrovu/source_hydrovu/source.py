#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator, Oauth2Authenticator
#from airbyte_cdk.sources.streams import IncrementalMixin
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

import json

"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.json file.
"""


# Basic full refresh stream
class HydroVuStream(HttpStream, ABC):
    """
    TODO remove this comment

    This class represents a stream output by the connector.
    This is an abstract base class meant to contain all the common functionality at the API level e.g: the API base URL, pagination strategy,
    parsing responses etc..

    Each stream should extend this class (or another abstract subclass of it) to specify behavior unique to that stream.

    Typically for REST APIs each stream corresponds to a resource in the API. For example if the API
    contains the endpoints
        - GET v1/customers
        - GET v1/employees

    then you should have three classes:
    `class HydrovuStream(HttpStream, ABC)` which is the current class
    `class Customers(HydrovuStream)` contains behavior to pull data for customers using v1/customers
    `class Employees(HydrovuStream)` contains behavior to pull data for employees using v1/employees

    If some streams implement incremental sync, it is typical to create another class
    `class IncrementalHydrovuStream((HydrovuStream), ABC)` then have concrete stream implementations extend it. An example
    is provided below.

    See the reference docs for the full list of configurable options.
    """

    # TODO: Fill in the url base. Required.
    url_base = "https://www.hydrovu.com/public-api/v1/"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        TODO: Override this method to define a pagination strategy. If you will not be using pagination, no action is required - just return None.

        This method should return a Mapping (e.g: dict) containing whatever information required to make paginated requests. This dict is passed
        to most other methods in this class to help you form headers, request bodies, query params, etc..

        For example, if the API accepts a 'page' parameter to determine which page of the result to return, and a response from the API contains a
        'page' number, then this method should probably return a dict {'page': response.json()['page'] + 1} to increment the page count by 1.
        The request_params method should then read the input next_page_token and set the 'page' param to next_page_token['page'].

        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        return None

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """
        TODO: Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
        Usually contains common params e.g. pagination size etc.
        """
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        yield {}


class Locations(HydroVuStream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "id"

    def path(
            self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "locations/list"


    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:

        def format_record(r):

            gps = r['gps']
            del r['gps']
            r['latitude'] = gps['latitude']
            r['longitude'] = gps['longitude']
            return r

        yield from (format_record(r) for r in response.json())


#class Readings(HydroVuStream, IncrementalMixin):
class Readings(HydroVuStream):
    primary_key = 'id'
    #cursor_field = 'timestamp'
    #cursor_field = {}
    #cursor_field = "date"
    #primary_key = "date"
    cursor_field = "latest_timestamp_for_each_location_param_id_combo"


    def __init__(self, auth, *args, **kw):
    #def __init__(self, auth, stream_state: Mapping[str, Any], *args, **kw):
        super(Readings, self).__init__(*args, **kw)

        locations = Locations(authenticator=auth)

        records = list(locations.read_records('full-refresh'))

        self._pages = (location for location in sorted(location['id'] for location in records))

        #if not self._cursor_value:
        #    print ("NEW CURSOR VALUE")
        
        #self._cursor_value = {}

        #start_date = "bbb"
        start_date = "ddd"

        self.start_date = start_date
        self._cursor_value = None

        self.location_and_param_id_latest_timestamps = {} 

        print ("Finish Init")



    def path(
            self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """

        # next_page_token is the location id
        if not next_page_token:
            next_page_token = next(self._pages)

        return f"locations/{next_page_token}/data"


    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        try:
            r = next(self._pages)

            return r
        except StopIteration:
            pass



    '''
    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:

        if not next_page_token:
            next_page_token = next(self._pages)

        self._active_page = next_page_token
        
        #td = datetime.timedelta(days=1)
        
        params = {'id': next_page_token}

        #params = {'id': next_page_token,
        #          'start': 0,
        #          'end': int((datetime.datetime.now() - td).timestamp() * 1000)}

        self._request_params_hook(params)
        return params
    '''
    
    @property
    def state(self) -> Mapping[str, Any]:
        print ("2222222222222222222222%%%%%%%%%%")
        print (self)
        print ("2222222222222222222222%%%%%%%%%%")


        return self.location_and_param_id_latest_timestamps

        '''
        if self._cursor_value:
            print ("CURSOR VALUE EXISTS")

            print (self._cursor_value)
            print ("55555555552222222222222222222222%%%%%%%%%%")

            #return {self.cursor_field: self._cursor_value.strftime('%Y-%m-%d')}
            return {self.cursor_field: self._cursor_value}
            #return

        else:
            print ("CURSOR VALUE DOES NOT EXISTS")
            #return {self.cursor_field: self.start_date.strftime('%Y-%m-%d')}
            return {self.cursor_field: self.start_date}
            #return
    
        '''





        '''
        for key, value in stream_state.items():
            print (key)
            print (value)

            print ("33333333333##############")
        '''

        '''
        if self._cursor_value:
            return {self.cursor_field: self._cursor_value.strftime('%Y-%m-%d')}
        else:
            return {self.cursor_field: self.start_date.strftime('%Y-%m-%d')}
        '''



    @state.setter
    def state(self, value: Mapping[str, Any]):
       #self._cursor_value = datetime.strptime(value[self.cursor_field], '%Y-%m-%d')
       #self._cursor_value = "ccc"

       print ("Setter")
       self._cursor_value = self.cursor_field

       print ("self._cursor_value")
       print (self._cursor_value)
       print ("WWWWWWWWWWWWWWWWWWWWWW")
       print ("self.cursor_field")
       print (self.cursor_field)
       print ("3333333333WWWWWWWWWWWWWWWWWWWWWW")

    '''
    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, any]:
        # This method is called once for each record returned from the API to compare the cursor field value in that record with the current state
        # we then return an updated state object. If this is the first time we run a sync or no state was passed, current_stream_state will be None.
        #if current_stream_state is not None and "date" in current_stream_state:
        if current_stream_state is not None and "a" in current_stream_state:
            #current_parsed_date = datetime.strptime(current_stream_state["date"], "%Y-%m-%d")
            #latest_record_date = datetime.strptime(latest_record["date"], "%Y-%m-%d")
            #return {"date": max(current_parsed_date, latest_record_date).strftime("%Y-%m-%d")}
            
            print ("hi!!!!!!!!!!!")

            return

        else:
            print ("2222hi!!!!!!!!!!!")
            return
            #return {"date": self.start_date.strftime("%Y-%m-%d")}
    '''


    #def update_state(state):
    #def update_state(self, state):
    def update_state(self):
        "sends an update of the state variable to stdout"
        #output_message = {"type":"STATE","state":{"data":state}}
        output_message = {"type":"STATE","state":{"data":self.location_and_param_id_latest_timestamps}}
        
        print(json.dumps(output_message))
        #print(output_message)


    def parse_response(self, 
        response: requests.Response, 
        stream_state: Mapping[str, Any],
        #stream_state,
        **kwargs) -> Iterable[Mapping]:

        
        print ("self._cursor_value")
        print (self._cursor_value)
        print ("zzzzzzzzzzzzzzz")


        # parse response.json into an iterable list of timestamps, values, and other params and yield that iterable list




        if stream_state is not None:

            a = 1
            print ("STREAM STATE EXISTS-------------------------------")
            #print (stream_state.get["a"])

            #stream_state['a'] = 1
            #stream_state.cursor = 1
            #print (stream_state['a'])
            print (stream_state)

            for key, value in stream_state.items():
                print (key)
                print (value)

            print ("##############")

        else:
            #stream_state['a'] = 1
            a = 1

            print ("STREAM STATE DOES NOT EXISTS-------------------------------")
            #print (stream_state['a'])
            print (stream_state)
             

        #self.update_state(stream_state)

        # Check to see if self.location_and_param_id_latest_timestamps is an empty dictionary
        if not bool(self.location_and_param_id_latest_timestamps):
            print ("Empty dict")

            self.location_and_param_id_latest_timestamps = stream_state
        else:

            print ("Not Empty dict")


        #print ("111111111RRRRRRRRRRRRRRR")
        #print(self.location_and_param_id_latest_timestamps)

        print ("RRRRRRRRRRRRRRR")
        print ("self.location_and_param_id_latest_timestamps")
        print (self.location_and_param_id_latest_timestamps)
        print ("22222RRRRRRRRRRRRRRR")



        response_json = response.json() 

        r = response.json()

        flat_readings_list = []
        
        locationId = r['locationId']

        parameters = r['parameters']



        #self._cursor_value[str(locationId)] = {}

        print ("AAAAAAAAAAAAA")
        print (locationId)



        


        if str(locationId) not in self.location_and_param_id_latest_timestamps.keys():
            print ("NO LOCATION ID")
            self.location_and_param_id_latest_timestamps[str(locationId)] = {}

        else:
            print ("YES LOCATION ID")


        #self.location_and_param_id_latest_timestamps[str(locationId)][str(parameterId)] = timestamp





        for param in parameters:

            parameterId = param['parameterId']

            unitId = param['unitId']

            customParameter = param['customParameter']

            readings = param['readings']

            print ("parameterId")
            print (parameterId)

            previous_timestamp = 0

            for reading in readings:

                timestamp = reading['timestamp']

                try:
                    cursor_timestamp = self.location_and_param_id_latest_timestamps[str(locationId)][str(parameterId)]
                    print ("cursor_timestamp exists")
                except:
                    cursor_timestamp = 0

                    print ("cursor_timestamp not exists")
                

                if timestamp > cursor_timestamp:
                    print ("greater than cursor_timestamp")


                

                    print ("timestamp")
                    print (timestamp)




                    value = reading['value']

                    flat_reading = {}

                    flat_reading['locationId'] = locationId
                    flat_reading['parameterId'] = parameterId
                    flat_reading['unitId'] = unitId
                    flat_reading['customParameter'] = customParameter
                    flat_reading['timestamp'] = timestamp
                    flat_reading['value'] = value

                    # More efficient way to do this???
                    flat_readings_list.append(flat_reading)

                    #if self._cursor_value:
                    #    self._cursor_value[locationId][parameterId] = timestamp



                    # Check to ensure that cursor_value is set to the latest timestamp.
                    # The records thus far have been read in sequential order by increasing
                    # timestamps, but this will ensure the latest timestamp if they are read
                    # out of order from the API.
                    if timestamp > previous_timestamp:
                        #self._cursor_value[str(locationId)][str(parameterId)] = timestamp
                        self.location_and_param_id_latest_timestamps[str(locationId)][str(parameterId)] = timestamp

                    else:
                        print ("WARNING TIMESTAMP IS NOT GREATER THAN PREVIOUS TIMESTAMP")   
                        #self._cursor_value[str(locationId)][str(parameterId)] = previous_timestamp
                        self.location_and_param_id_latest_timestamps[str(locationId)][str(parameterId)] = previous_timestamp


                    previous_timestamp = timestamp


                    #self._cursor_value[str(locationId)] = timestamp
                
                else:
                    print ("less than cursor_timestamp")


        #self.update_state(stream_state)
        
        #self.update_state()


        print ("nest dict")
        print ("--------------------------------")
        #for key, value in self._cursor_value.items():
        for key, value in self.location_and_param_id_latest_timestamps.items():
            print (str(key) + " " + str(value))
        print ("--------------------------------")

        '''
        print ("--------------------------------")
        for key, value in self._cursor_value.items():
            print ("*************")
            for key2, value2 in value.items():
            
               print (str(key) + " " + str(key2) + " "  + str(value2))
        print ("--------------------------------")
        '''


        '''
        print ("--------------------------------")
        for key in self._cursor_value:
            print (str(key))
        print ("--------------------------------")
        '''



        yield from flat_readings_list


    '''
    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, any]:
        # This method is called once for each record returned from the API to compare the cursor field value in that record with the current state
        # we then return an updated state object. If this is the first time we run a sync or no state was passed, current_stream_state will be None.


        #if current_stream_state is not None and "date" in current_stream_state:
        if current_stream_state is not None:


            current_parsed_date = datetime.strptime(current_stream_state["date"], "%Y-%m-%d")
            latest_record_date = datetime.strptime(latest_record["date"], "%Y-%m-%d")
            return {"date": max(current_parsed_date, latest_record_date).strftime("%Y-%m-%d")}
        else:


            return {"date": self.start_date.strftime("%Y-%m-%d")}
    '''




class myOauth2Authenticator(Oauth2Authenticator):

    def refresh_access_token(self) -> Tuple[str, int]:
        """
        returns a tuple of (access_token, token_lifespan_in_seconds)
        """
        client = BackendApplicationClient(client_id=self.client_id)
        oauth = OAuth2Session(client=client)
        response_json = oauth.fetch_token(token_url=self.token_refresh_endpoint,
                                          client_id=self.client_id,
                                          client_secret=self.client_secret)
        return response_json[self.access_token_name], response_json[self.expires_in_name]


# Source
class SourceHydrovu(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        TODO: Implement a connection check to validate that the user-provided config can be used to connect to the underlying API

        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.json
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        try:

            # create session
            client = BackendApplicationClient(client_id=config["client_id"])
            oauth = OAuth2Session(client=client)
            token = oauth.fetch_token(token_url='https://www.hydrovu.com/public-api/oauth/token',
                                      client_id=config["client_id"],
                                      client_secret=config["client_secret"])

            return True, None

        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        TODO: Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """

        auth = myOauth2Authenticator(
                                     config['token_refresh_endpoint'],
                                     config['client_id'],
                                     config['client_secret'],
                                     ""
                                     )

        return [Locations(authenticator=auth), Readings(auth, authenticator=auth)]
