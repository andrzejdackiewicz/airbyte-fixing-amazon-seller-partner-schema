#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#
from logging import Logger
from typing import Any, List, Mapping, Tuple

import pendulum
import requests
from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.yaml_declarative_source import YamlDeclarativeSource
from airbyte_cdk.sources.streams.core import Stream
from airbyte_cdk.sources.streams.http.auth import BasicHttpAuthenticator
from airbyte_cdk.utils.traced_exception import AirbyteTracedException
from pydantic.error_wrappers import ValidationError

from .streams import IssueComments, IssueFields, Issues, IssueWorklogs, PullRequests
from .utils import read_full_refresh


class SourceJira(YamlDeclarativeSource):
    def __init__(self):
        super().__init__(**{"path_to_yaml": "manifest.yaml"})

    def check_connection(self, logger: Logger, config: Mapping[str, Any]) -> Tuple[bool | Any]:
        try:
            streams = self.streams(config)
            stream_name_to_stream = {s.name: s for s in streams}

            # check projects
            if config.get("projects"):
                projects_stream = stream_name_to_stream["projects"]
                actual_projects = {project["key"] for project in read_full_refresh(projects_stream)}
                unknown_projects = set(config["projects"]) - actual_projects
                if unknown_projects:
                    return False, "unknown project(s): " + ", ".join(unknown_projects)

            # Get streams to check access to any of them
            for stream_name in self._source_config["check"]["stream_names"]:
                try:
                    data = next(read_full_refresh(stream_name_to_stream[stream_name]), None)
                    print(data)
                except:
                    logger.warning(f"No access to stream: {stream_name}")
                else:
                    logger.info(f"API Token have access to stream: {stream_name}, so check is successful.")
                    return True, None
            return False, "This API Token does not have permission to read any of the resources."
        except ValidationError as validation_error:
            return False, validation_error
        except requests.exceptions.RequestException as request_error:
            has_response = request_error.response is not None
            is_invalid_domain = (
                isinstance(request_error, requests.exceptions.InvalidURL)
                or has_response
                and request_error.response.status_code == requests.codes.not_found
            )

            if is_invalid_domain:
                raise AirbyteTracedException(
                    message="Config validation error: please check that your domain is valid and does not include protocol (e.g: https://).",
                    internal_message=str(request_error),
                    failure_type=FailureType.config_error,
                ) from None

            # sometimes jira returns non json response
            if has_response and request_error.response.headers.get("content-type") == "application/json":
                message = " ".join(map(str, request_error.response.json().get("errorMessages", "")))
                return False, f"{message} {request_error}"

            # we don't know what this is, rethrow it
            raise request_error

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        streams = super().streams(config)
        return streams + self.get_non_portable_streams(config=config)

    def _validate_and_transform_config(self, config: Mapping[str, Any]):
        start_date = config.get("start_date")
        if start_date:
            config["start_date"] = pendulum.parse(start_date)
        config["lookback_window_minutes"] = pendulum.duration(minutes=config.get("lookback_window_minutes", 0))
        config["projects"] = config.get("projects", [])
        return config

    @staticmethod
    def get_authenticator(config: Mapping[str, Any]):
        return BasicHttpAuthenticator(config["email"], config["api_token"])

    def get_non_portable_streams(self, config: Mapping[str, Any]) -> List[Stream]:
        config = self._validate_and_transform_config(config.copy())
        authenticator = self.get_authenticator(config)
        args = {"authenticator": authenticator, "domain": config["domain"], "projects": config["projects"]}
        incremental_args = {
            **args,
            "start_date": config.get("start_date"),
            "lookback_window_minutes": config.get("lookback_window_minutes"),
        }
        issues_stream = Issues(**incremental_args)
        issue_fields_stream = IssueFields(**args)

        streams = [IssueComments(**incremental_args), IssueWorklogs(**incremental_args)]

        experimental_streams = []
        if config.get("enable_experimental_streams", False):
            experimental_streams.append(
                PullRequests(issues_stream=issues_stream, issue_fields_stream=issue_fields_stream, **incremental_args)
            )
        return streams + experimental_streams
