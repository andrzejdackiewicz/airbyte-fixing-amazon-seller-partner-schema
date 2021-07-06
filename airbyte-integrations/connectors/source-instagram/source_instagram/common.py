#
# MIT License
#
# Copyright (c) 2020 Airbyte
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#


import sys
import urllib.parse as urlparse

import backoff
from airbyte_cdk.entrypoint import logger  # FIXME (Eugene K): register logger as standard python logger
from requests.status_codes import codes as status_codes


class InstagramAPIException(Exception):
    """General class for all API errors"""


def retry_pattern(backoff_type, exception, **wait_gen_kwargs):
    def log_retry_attempt(details):
        _, exc, _ = sys.exc_info()
        logger.info(str(exc))
        logger.info(f"Caught retryable error after {details['tries']} tries. Waiting {details['wait']} more seconds then retrying...")

    def should_retry_api_error(exc):
        if exc.http_status() in (status_codes.TOO_MANY_REQUESTS, status_codes.BAD_REQUEST):
            return True

        if exc.http_status() == status_codes.FORBIDDEN and exc.api_error_message() == "(#4) Application request limit reached":
            return True

        # Issue 4028, Sometimes an error about the Rate Limit is returned with a 400 HTTP code
        if exc.http_status() == status_codes.BAD_REQUEST:
            return True

        if exc.api_transient_error():
            return True

        return False

    return backoff.on_exception(
        backoff_type,
        exception,
        jitter=None,
        on_backoff=log_retry_attempt,
        giveup=lambda exc: not should_retry_api_error(exc),
        **wait_gen_kwargs,
    )


def remove_params_from_url(url, params):
    parsed_url = urlparse.urlparse(url)
    res_query = []
    for q in parsed_url.query.split("&"):
        key, value = q.split("=")
        if key not in params:
            res_query.append(f"{key}={value}")

    parse_result = parsed_url._replace(query="&".join(res_query))
    return urlparse.urlunparse(parse_result)
