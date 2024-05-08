#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from typing import Optional, Union

import requests


class BaseBackoffException(requests.exceptions.HTTPError):
    def __init__(
        self,
        request: requests.PreparedRequest,
        response_or_exception: Optional[Union[requests.Response, requests.RequestException]],
        error_message: str = "",
    ):

        if isinstance(response_or_exception, requests.Response):
            error_message = (
                error_message
                or f"Request URL: {request.url}, Response Code: {response_or_exception.status_code}, Response Text: {response_or_exception.text}"
            )
            super().__init__(error_message, request=request, response=response_or_exception)
        else:
            error_message = error_message or f"Request URL: {request.url}, Exception: {response_or_exception}"
            super().__init__(error_message, request=request, response=None)


class RequestBodyException(Exception):
    """
    Raised when there are issues in configuring a request body
    """


class UserDefinedBackoffException(BaseBackoffException):
    """
    An exception that exposes how long it attempted to backoff
    """

    def __init__(
        self,
        backoff: Union[int, float],
        request: requests.PreparedRequest,
        response_or_exception: Optional[Union[requests.Response, requests.RequestException]],
        error_message: str = "",
    ):
        """
        :param backoff: how long to backoff in seconds
        :param request: the request that triggered this backoff exception
        :param response: the response that triggered the backoff exception
        """
        self.backoff = backoff
        super().__init__(request=request, response_or_exception=response_or_exception, error_message=error_message)


class DefaultBackoffException(BaseBackoffException):
    pass
