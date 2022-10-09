#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import json
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

import vertica_python


class SourceVertica(Source):
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
            vertica_python.connect(
                host=config['host'],
                port=config['port'],
                user=config['user'],
                password=config['password'],
                database=config['database'],
                schema_name=config['schema_name']
            )

            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {str(e)}")

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

        connection = vertica_python.connect(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            password=config['password'],
            database=config['database'],
            schema_name=config['schema_name']
        )

        tables = self.get_tables(logger, config, connection)

        for table in tables:
            properties = {}
            columns = self.get_columns(logger, config, connection, table)

            for column in columns:
                properties[column[0]] = {
                    "type": column[1]
                }

            streams.append(
                AirbyteStream(
                    name=table,
                    json_schema={
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "type": "object",
                        "properties": properties,
                    }
                )
            )

        return AirbyteCatalog(streams=streams)

    def get_tables(self, logger: AirbyteLogger, config: json, connection):
        query = f'''
            SELECT 
                TABLE_NAME
            FROM v_catalog.tables 
            WHERE table_schema='{config['schema_name']}' 
            ORDER BY table_name;
        '''

        results = self.query(logger, config, connection, query)

        return [result[0] for result in results]

    def get_columns(self, logger: AirbyteLogger, config: json, connection, table_name: str):
        query = f'''
            SELECT 
                column_name , 
                data_type 
            FROM v_catalog.columns 
            WHERE table_name='{table_name}';'''

        results = self.query(logger, config, connection, query)

        return results

    def query(self, logger: AirbyteLogger, config: json, connection, query):
        with connection.cursor() as cursor:
            try:
                cursor.execute(query)
                results = cursor.fetchall()
            except Exception as e:
                logger.error(f'Error running query: {query} on {config["database"]}!')

        return results

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
