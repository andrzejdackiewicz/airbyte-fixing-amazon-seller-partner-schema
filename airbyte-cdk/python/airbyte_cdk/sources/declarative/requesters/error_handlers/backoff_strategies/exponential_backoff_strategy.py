#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass
from typing import Any, Callable, Mapping, Optional, Union

import requests
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.declarative.models.declarative_component_schema import (
    ExponentialBackoffStrategy as ExponentialBackoffStrategyModel,
)
from airbyte_cdk.sources.declarative.parsers.component_constructor import ComponentConstructor
from airbyte_cdk.sources.streams.http.error_handlers import BackoffStrategy
from airbyte_cdk.sources.types import Config
from pydantic import BaseModel


@dataclass
class ExponentialBackoffStrategy(BackoffStrategy, ComponentConstructor):
    """
    Backoff strategy with an exponential backoff interval

    Attributes:
        factor (float): multiplicative factor
    """

    parameters: InitVar[Mapping[str, Any]]
    config: Config
    factor: Union[float, InterpolatedString, str] = 5

    @classmethod
    def resolve_dependencies(
        cls,
        model: ExponentialBackoffStrategyModel,
        config: Config,
        **kwargs: Any,
    ) -> Optional[Mapping[str, Any]]:
        return {"factor": model.factor or 5, "parameters": model.parameters or {}, "config": config}

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        if not isinstance(self.factor, InterpolatedString):
            self.factor = str(self.factor)
        if isinstance(self.factor, float):
            self._factor = InterpolatedString.create(str(self.factor), parameters=parameters)
        else:
            self._factor = InterpolatedString.create(self.factor, parameters=parameters)

    @property
    def _retry_factor(self) -> float:
        return self._factor.eval(self.config)  # type: ignore # factor is always cast to an interpolated string

    def backoff_time(
        self,
        response_or_exception: Optional[Union[requests.Response, requests.RequestException]],
        attempt_count: int,
    ) -> Optional[float]:
        return self._retry_factor * 2**attempt_count  # type: ignore # factor is always cast to an interpolated string
