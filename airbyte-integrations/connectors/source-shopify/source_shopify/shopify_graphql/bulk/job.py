#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from dataclasses import dataclass, field
from enum import Enum
from time import sleep, time
from typing import Any, ClassVar, Iterable, List, Mapping, Optional, Union

import requests
from airbyte_cdk import AirbyteLogger
from requests.exceptions import JSONDecodeError
from source_shopify.utils import ApiTypeEnum
from source_shopify.utils import ShopifyRateLimiter as limiter

from .exceptions import AirbyteTracedException, ShopifyBulkExceptions
from .query import ShopifyBulkTemplates
from .tools import END_OF_FILE, BulkTools


class ShopifyBulkStatus(Enum):
    CREATED = "CREATED"
    CANCELED = "CANCELED"
    COMPLETED = "COMPLETED"
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"
    ACCESS_DENIED = "ACCESS_DENIED"


@dataclass
class ShopifyBulkManager:
    session: requests.Session
    base_url: str

    # default logger
    logger: ClassVar[AirbyteLogger] = logging.getLogger("airbyte")

    # 10Mb chunk size to save the file
    retrieve_chunk_size: ClassVar[int] = 1024 * 1024 * 10
    # time between job status checks
    job_check_interval_sec: ClassVar[int] = 5

    # PLATFORM HEARTBEAT NOTES:
    # 30 sec / attempt * 19 attempts = 570 sec of wait time in total,
    # which is < 10 min of retrying, before Heartbeat will kill the source as non-responsive

    # sleep time per creation attempt
    concurrent_interval_sec: ClassVar[int] = 30
    # max attempts for job creation
    concurrent_max_retry: ClassVar[int] = 19
    # Each job ideally should be executed within the specified time (in sec),
    # to maximize the performance for multi-connection syncs and control the bulk job size within +- 7 mins (420 sec),
    # Ideally the source will balance on it's own rate, based on the time taken to return the data for the slice.
    job_elapsed_time_threshold_sec: ClassVar[float] = 420.0
    # 0.1 ~= P2H, default value, lower boundary for slice size
    job_slice_interval_in_days_min: ClassVar[float] = 0.1
    # P365D, upper boundary for slice size
    job_slice_interval_in_days_max: ClassVar[float] = 365.0
    # running job logger constrain
    log_running_job_msg_frequency: ClassVar[int] = 3
    # attempt limit indicator
    concurrent_max_attempt_reached: bool = field(init=False, default=False)
    # attempt counter
    concurrent_attempt: int = field(init=False, default=0)
    # currents: job_id, job_state
    job_id: Optional[str] = field(init=False, default=None)
    job_state: ShopifyBulkStatus = field(init=False, default=None)
    # 2 sec is set as default value to cover the case with the empty-fast-completed jobs
    job_last_elapsed_time_sec: float = field(init=False, default=2.0)
    # dynamically adjusted slice interval
    job_slice_interval: float = field(init=False, default=0.0)
    # expand slice factor
    slice_size_expand_factor: int = field(init=False, default=2)
    # reduce slice factor
    slice_size_reduce_factor: int = field(init=False, default=2)
    # running job log counter
    log_running_job_msg_count: int = field(init=False, default=0)

    @property
    def tools(self) -> BulkTools:
        return BulkTools()

    @property
    def job_state_to_fn_map(self) -> Mapping[str, Any]:
        return {
            ShopifyBulkStatus.CREATED.value: self.on_created_job,
            ShopifyBulkStatus.CANCELED.value: self.on_canceled_job,
            ShopifyBulkStatus.COMPLETED.value: self.on_completed_job,
            ShopifyBulkStatus.RUNNING.value: self.on_running_job,
            ShopifyBulkStatus.TIMEOUT.value: self.on_timeout_job,
            ShopifyBulkStatus.FAILED.value: self.on_failed_job,
            ShopifyBulkStatus.ACCESS_DENIED.value: self.on_access_denied_job,
        }

    def __reset_state(self) -> None:
        # set current job state to default value
        self.job_state, self.job_id = None, None
        # set the running job message counter to default
        self.log_running_job_msg_count = 0

    def job_completed(self) -> bool:
        return self.job_state == ShopifyBulkStatus.COMPLETED.value

    def log_running_job_state(self) -> None:
        """
        Print the `RUNNING` Job info message every 3 request, to minimize the noise in the logs.
        """
        if self.log_running_job_msg_count < self.log_running_job_msg_frequency:
            self.log_running_job_msg_count += 1
        else:
            self.log_state()
            self.log_running_job_msg_count = 0

    def log_state(self) -> None:
        self.logger.info(f"The BULK Job: `{self.job_id}` is {self.job_state}.")

    def job_get_state_args(self) -> Mapping[str, Any]:
        return {
            "method": "POST",
            "url": self.base_url,
            "data": ShopifyBulkTemplates.status(self.job_id),
            "headers": {"Content-Type": "application/graphql"},
        }

    def job_get_result(self, response: Optional[requests.Response] = None) -> Optional[str]:
        job_result_url = response.json().get("data", {}).get("node", {}).get("url") if response else None
        if job_result_url:
            # save to local file using chunks to avoid OOM
            filename = self.tools.filename_from_url(job_result_url)
            with self.session.get(job_result_url, stream=True) as response:
                response.raise_for_status()
                with open(filename, "wb") as file:
                    for chunk in response.iter_content(chunk_size=self.retrieve_chunk_size):
                        file.write(chunk)
                    # add `<end_of_file>` line to the bottom  of the saved data for easy parsing
                    file.write(END_OF_FILE.encode())
            return filename

    def job_update_state(self, response: Optional[requests.Response] = None) -> None:
        if response:
            self.job_state = response.json().get("data", {}).get("node", {}).get("status")
            if self.job_state == ShopifyBulkStatus.RUNNING.value:
                self.log_running_job_state()
            else:
                self.log_state()

    def on_created_job(self, **kwargs) -> None:
        pass

    def on_canceled_job(self, response: requests.Response) -> AirbyteTracedException:
        raise ShopifyBulkExceptions.BulkJobCanceled(
            f"The BULK Job: `{self.job_id}` exited with {self.job_state}, details: {response.text}",
        )

    def on_running_job(self, **kwargs) -> None:
        sleep(self.job_check_interval_sec)

    def on_completed_job(self, **kwargs) -> None:
        pass

    def on_failed_job(self, response: requests.Response) -> AirbyteTracedException:
        raise ShopifyBulkExceptions.BulkJobFailed(
            f"The BULK Job: `{self.job_id}` exited with {self.job_state}, details: {response.text}",
        )

    def on_timeout_job(self, **kwargs) -> AirbyteTracedException:
        raise ShopifyBulkExceptions.BulkJobTimout(
            f"The BULK Job: `{self.job_id}` exited with {self.job_state}, please reduce the `GraphQL BULK Date Range in Days` in SOURCES > Your Shopify Source > SETTINGS.",
        )

    def on_access_denied_job(self, **kwagrs) -> AirbyteTracedException:
        raise ShopifyBulkExceptions.BulkJobAccessDenied(
            f"The BULK Job: `{self.job_id}` exited with {self.job_state}, please check your PERMISSION to fetch the data for this stream.",
        )

    def on_job_with_errors(self, errors: List[Mapping[str, Any]]) -> AirbyteTracedException:
        raise ShopifyBulkExceptions.BulkJobUnknownError(f"Could not validate the status of the BULK Job `{self.job_id}`. Errors: {errors}.")

    def job_check_for_errors(self, response: requests.Response) -> Iterable[Mapping[str, Any]]:
        try:
            return response.json().get("errors") or response.json().get("data", {}).get("bulkOperationRunQuery", {}).get("userErrors", [])
        except (Exception, JSONDecodeError) as e:
            raise ShopifyBulkExceptions.BulkJobBadResponse(
                f"Couldn't check the `response` for `errors`, status: {response.status_code}, response: `{response.text}`. Trace: {repr(e)}."
            )

    def job_track_running(self) -> Union[AirbyteTracedException, requests.Response]:
        # format Job state check args
        status_args = self.job_get_state_args()
        # re-use of `self._session(*, **)` to make BULK Job status checks
        response = self.session.request(**status_args)
        # errors check
        errors = self.job_check_for_errors(response)
        if not errors:
            self.job_update_state(response)
            self.job_state_to_fn_map.get(self.job_state)(response=response)
            return response
        else:
            # execute ERRORS scenario
            self.on_job_with_errors(errors)

    def job_check_state(self) -> Optional[str]:
        while not self.job_completed():
            response = self.job_track_running()
        # return `job_result_url` when status is `COMPLETED`
        return self.job_get_result(response)

    def has_running_concurrent_job(self, errors: Optional[Iterable[Mapping[str, Any]]] = None) -> bool:
        """
        When concurent BULK Job is already running for the same SHOP we receive:
        Error example:
        [
            {
                'field': None,
                'message': 'A bulk query operation for this app and shop is already in progress: gid://shopify/BulkOperation/4039184154813.',
            }
        ]
        """

        concurent_job_pattern = "A bulk query operation for this app and shop is already in progress"
        # the errors are handled in `job_job_check_for_errors`
        if errors:
            for error in errors:
                message = error.get("message", "") if isinstance(error, dict) else ""
                if concurent_job_pattern in message:
                    return True
        # reset the `concurrent_attempt` counter, once there is no concurrent job error
        self.concurrent_attempt = 0
        return False

    def has_reached_max_concurrency_attempt(self) -> bool:
        return self.concurrent_attempt == self.concurrent_max_retry

    def job_retry_request(self, request: requests.PreparedRequest) -> Optional[requests.Response]:
        # retry current `request`
        return self.session.send(request)

    def job_retry_concurrent(self, request: requests.PreparedRequest) -> Optional[requests.Response]:
        # increment attempt
        self.concurrent_attempt += 1
        # try to execute previous request, it's handy because we can retry / each slice yielded
        self.logger.warning(
            f"The BULK concurrency limit has reached. Waiting {self.concurrent_interval_sec} sec before retry, atttempt: {self.concurrent_attempt}.",
        )
        sleep(self.concurrent_interval_sec)
        # retry current `request`
        return self.job_healthcheck(self.job_retry_request(request))

    def job_get_id(self, response: requests.Response) -> Optional[str]:
        response_data = response.json()
        bulk_response = response_data.get("data", {}).get("bulkOperationRunQuery", {}).get("bulkOperation", {})
        if bulk_response and bulk_response.get("status") == ShopifyBulkStatus.CREATED.value:
            job_id = bulk_response.get("id")
            self.logger.info(f"The BULK Job: `{job_id}` is {ShopifyBulkStatus.CREATED.value}")
            return job_id
        else:
            return None

    def job_retry_on_concurrency(self, request: requests.PreparedRequest) -> Union[AirbyteTracedException, Optional[requests.Response]]:
        if self.has_reached_max_concurrency_attempt():
            # indicate we're out of attempts to retry with job creation
            message = f"The BULK Job couldn't be created at this time, since another job is running."
            # log the message
            self.logger.error(message)
            # raise AibyteTracebackException with `INCOMPLETE` status
            raise ShopifyBulkExceptions.BulkJobConcurrentError(message)
        else:
            return self.job_retry_concurrent(request)

    def job_healthcheck(self, response: requests.Response) -> Optional[requests.Response]:
        # get the latest request to retry
        request: requests.PreparedRequest = response.request
        try:
            errors = self.job_check_for_errors(response)
            # when the concurrent job takes place, we typically need to wait and retry, but no longer than 10 min.
            if not self.has_running_concurrent_job(errors):
                return response if not errors else None
            else:
                return self.job_retry_on_concurrency(request)
        except (ShopifyBulkExceptions.BulkJobBadResponse, ShopifyBulkExceptions.BulkJobUnknownError) as err:
            # sometimes we face with `HTTP-500 Internal Server Error`
            # we should retry such at least once
            return self.job_retry_request(request)

    def _calculate_slice_expand_factor(self, coef: float = 0.5) -> float:
        """
        The expand factor is calculated using EMA (Expotentional Moving Average):
            coef - the expantion coefficient
            previous_expand_factor - previous factor value

        Formula: expand_factor = coef * previous_expand_factor + (1 - coef)
        """

        return coef * self.slice_size_expand_factor + (1 - coef)

    def _calculate_slice_reduce_factor(self) -> float:
        """
        The reduce factor is 2, by default.
        """

        return self.slice_size_reduce_factor

    def _adjust_slice_size(self, current_job_elapsed_time: float) -> None:
        if current_job_elapsed_time > self.job_elapsed_time_threshold_sec:
            # print(current_job_elapsed_time, self.job_elapsed_time_threshold_sec)
            self.job_slice_interval /= self._calculate_slice_reduce_factor()
        elif current_job_elapsed_time < 1 or current_job_elapsed_time < self.job_last_elapsed_time_sec:
            self.job_slice_interval += self._calculate_slice_expand_factor()
        elif current_job_elapsed_time > self.job_last_elapsed_time_sec < self.job_elapsed_time_threshold_sec:
            pass

        # set the last job time
        self.job_last_elapsed_time_sec = current_job_elapsed_time
        # check the slice interval are acceptable
        self.job_slice_interval = max(
            self.job_slice_interval_in_days_min, min(self.job_slice_interval, self.job_slice_interval_in_days_max)
        )

    @limiter.balance_rate_limit(api_type=ApiTypeEnum.graphql.value)
    def job_check(self, created_job_response: requests.Response) -> Optional[str]:
        """
        This method checks the status for the BULK Job created, using it's `ID`.
        The time spent for the Job execution is tracked to understand the effort.
        """
        job_response = self.job_healthcheck(created_job_response)
        self.job_id: str = self.job_get_id(job_response)
        job_started = time()
        try:
            return self.job_check_state()
        except (
            ShopifyBulkExceptions.BulkJobCanceled,
            ShopifyBulkExceptions.BulkJobFailed,
            ShopifyBulkExceptions.BulkJobTimout,
            ShopifyBulkExceptions.BulkJobAccessDenied,
            ShopifyBulkExceptions.BulkJobUnknownError,
        ) as bulk_job_error:
            raise bulk_job_error
        finally:
            current_job_elapsed_time = round((time() - job_started), 3)
            self.logger.info(f"The BULK Job: `{self.job_id}` time elapsed: {current_job_elapsed_time} sec.")
            # check whether or not we should expand or reduce the size of the slice
            self._adjust_slice_size(current_job_elapsed_time)
            # reset the state for COMPLETED job
            self.__reset_state()
