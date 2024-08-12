#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass
from typing import Any, Callable, List, Mapping, Optional, Union

import requests
from airbyte_cdk.sources.declarative.models.declarative_component_schema import CompositeErrorHandler as CompositeErrorHandlerModel
from airbyte_cdk.sources.declarative.parsers.component_constructor import ComponentConstructor
from airbyte_cdk.sources.streams.http.error_handlers import ErrorHandler
from airbyte_cdk.sources.streams.http.error_handlers.response_models import DEFAULT_ERROR_RESOLUTION, ErrorResolution, ResponseAction
from airbyte_cdk.sources.types import Config
from pydantic import BaseModel


@dataclass
class CompositeErrorHandler(ErrorHandler, ComponentConstructor):
    """
    Error handler that sequentially iterates over a list of `ErrorHandler`s

    Sample config chaining 2 different retriers:
        error_handler:
          type: "CompositeErrorHandler"
          error_handlers:
            - response_filters:
                - predicate: "{{ 'codase' in response }}"
                  action: RETRY
              backoff_strategies:
                - type: "ConstantBackoff"
                  backoff_time_in_seconds: 5
            - response_filters:
                - http_codes: [ 403 ]
                  action: RETRY
              backoff_strategies:
                - type: "ConstantBackoff"
                  backoff_time_in_seconds: 10
    Attributes:
        error_handlers (List[ErrorHandler]): list of error handlers
    """

    error_handlers: List[ErrorHandler]
    parameters: InitVar[Mapping[str, Any]]

    @classmethod
    def resolve_dependencies(
        cls,
        model: CompositeErrorHandlerModel,
        config: Config,
        dependency_constructor: Callable[[BaseModel, Config], Any],
        additional_flags: Optional[Mapping[str, Any]] = None,
        **kwargs: Any,
    ) -> Optional[Mapping[str, Any]]:
        error_handlers = [dependency_constructor(model=error_handler_model, config=config) for error_handler_model in model.error_handlers]
        return {
            "error_handlers": error_handlers,
            "parameters": model.parameters or {},
        }

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        if not self.error_handlers:
            raise ValueError("CompositeErrorHandler expects at least 1 underlying error handler")

    @property
    def max_retries(self) -> Optional[int]:
        return self.error_handlers[0].max_retries

    @property
    def max_time(self) -> Optional[int]:
        return max([error_handler.max_time or 0 for error_handler in self.error_handlers])

    def interpret_response(self, response_or_exception: Optional[Union[requests.Response, Exception]]) -> ErrorResolution:
        matched_error_resolution = None
        for error_handler in self.error_handlers:
            matched_error_resolution = error_handler.interpret_response(response_or_exception)

            if not isinstance(matched_error_resolution, ErrorResolution):
                continue

            if matched_error_resolution.response_action == ResponseAction.SUCCESS:
                return matched_error_resolution

            if (
                matched_error_resolution.response_action == ResponseAction.RETRY
                or matched_error_resolution.response_action == ResponseAction.IGNORE
            ):
                return matched_error_resolution
        if matched_error_resolution:
            return matched_error_resolution

        return DEFAULT_ERROR_RESOLUTION
