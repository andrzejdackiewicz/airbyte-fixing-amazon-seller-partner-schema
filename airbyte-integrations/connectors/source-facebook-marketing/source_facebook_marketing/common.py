#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from datetime import datetime
import logging
import sys
from typing import Any, Iterable, Sequence

import backoff
import pendulum
from facebook_business.exceptions import FacebookRequestError

# The Facebook API error codes indicating rate-limiting are listed at
# https://developers.facebook.com/docs/graph-api/overview/rate-limiting/
FACEBOOK_RATE_LIMIT_ERROR_CODES = (4, 17, 32, 613, 80000, 80001, 80002, 80003, 80004, 80005, 80006, 80008)
FACEBOOK_UNKNOWN_ERROR_CODE = 99
DEFAULT_SLEEP_INTERVAL = pendulum.duration(minutes=1)

logger = logging.getLogger("airbyte")

class SourceFacebookMarketingConfig:
    format = "%Y-%m-%dT%H:%M:%SZ"

    def __init__(self, config):
        self.__config = config

    @property
    def config(self):
        return self.__config

    @property
    def access_token(self):
        return self.config["access_token"]

    @property
    def account_ids(self):
        return self.config["accounts"].get("ids", [])

    @property
    def account_selection_strategy(self):
        return self.config["accounts"]["selection_strategy"]

    @property
    def account_selection_strategy_is_subset(self):
        return self.account_selection_strategy == "subset"

    @property
    def account_selection_strategy_is_all(self):
        return self.account_selection_strategy == "all"

    @property
    def start_date(self):
        return datetime.strptime(self.config["start_date"], self.format)

    @property
    def end_date(self):
        return datetime.strptime(self.config["end_date"], self.format) if self.config.get("end_date") else pendulum.now()

    @property
    def include_deleted(self):
        return self.config["include_deleted"]

    @property
    def custom_insights(self):
        return self.config.get("custom_insights")

    @property
    def insights_lookback_window(self):
        return self.config["insights_lookback_window"]

    @property
    def insights_days_per_job(self):
        return self.config["insights_days_per_job"]


class FacebookAPIException(Exception):
    """General class for all API errors"""


class JobException(Exception):
    """Scheduled job failed"""


class JobTimeoutException(JobException):
    """Scheduled job timed out"""


def batch(iterable: Sequence, size: int = 1) -> Iterable:
    """Split sequence in chunks"""
    total_size = len(iterable)
    for ndx in range(0, total_size, size):
        yield iterable[ndx : min(ndx + size, total_size)]


def retry_pattern(backoff_type, exception, **wait_gen_kwargs):
    def log_retry_attempt(details):
        _, exc, _ = sys.exc_info()
        logger.info(str(exc))
        logger.info(f"Caught retryable error after {details['tries']} tries. Waiting {details['wait']} more seconds then retrying...")

    def should_retry_api_error(exc):
        if isinstance(exc, FacebookRequestError):
            call_rate_limit_error = exc.api_error_code() in FACEBOOK_RATE_LIMIT_ERROR_CODES
            return exc.api_transient_error() or exc.api_error_subcode() == FACEBOOK_UNKNOWN_ERROR_CODE or call_rate_limit_error
        return True

    return backoff.on_exception(
        backoff_type,
        exception,
        jitter=None,
        on_backoff=log_retry_attempt,
        giveup=lambda exc: not should_retry_api_error(exc),
        **wait_gen_kwargs,
    )


def deep_merge(a: Any, b: Any) -> Any:
    """Merge two values, with `b` taking precedence over `a`."""
    if isinstance(a, dict) and isinstance(b, dict):
        # set of all keys in both dictionaries
        keys = set(a.keys()) | set(b.keys())

        return {key: deep_merge(a.get(key), b.get(key)) for key in keys}
    elif isinstance(a, list) and isinstance(b, list):
        return [*a, *b]
    elif isinstance(a, set) and isinstance(b, set):
        return a | b
    else:
        return a if b is None else b
