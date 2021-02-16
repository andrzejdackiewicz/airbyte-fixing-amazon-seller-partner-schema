"""
MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import sys
import time

import backoff
import requests
from base_python.entrypoint import logger
from source_freshdesk.errors import FreshdeskRateLimited


def retry_connection_handler(exception, **wait_gen_kwargs):
    """Retry helper, log each attempt"""

    def log_retry_attempt(details):
        _, exc, _ = sys.exc_info()
        logger.info(str(exc))
        logger.info(f"Caught retryable error after {details['tries']} tries. Waiting {details['wait']} more seconds then retrying...")

    return backoff.on_exception(
        backoff.expo,
        exception,
        jitter=None,
        on_backoff=log_retry_attempt,
        giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500 ** wait_gen_kwargs,
    )


def retry_after_handler(max_tries):
    """Retry helper when we hit the call limit, sleeps for specific duration"""

    def log_retry_attempt(_details):
        _, exc, _ = sys.exc_info()
        if isinstance(exc, requests.exceptions.RequestException):
            retry_after = int(exc.response.headers["Retry-After"])
            logger.info(f"Rate limit reached. Sleeping for {retry_after} seconds")
            time.sleep(retry_after + 1)  # extra second to cover any fractions of second

    def log_giveup(_details):
        logger.error("Max retry limit reached")

    return backoff.on_exception(
        backoff.constant(0),
        FreshdeskRateLimited,
        max_tries=max_tries,
        jitter=None,
        on_backoff=log_retry_attempt,
        on_giveup=log_giveup,
    )
