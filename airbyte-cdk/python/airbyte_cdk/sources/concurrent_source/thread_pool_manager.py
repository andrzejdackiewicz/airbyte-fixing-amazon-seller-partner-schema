#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import logging
import threading

from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable, List, Optional


class ThreadPoolManager:
    """
    Wrapper to abstract away the threadpool and the logic to wait for pending tasks to be completed.
    """

    DEFAULT_MAX_QUEUE_SIZE = 10_000

    def __init__(
        self,
        threadpool: ThreadPoolExecutor,
        logger: logging.Logger,
        max_concurrent_tasks: int = DEFAULT_MAX_QUEUE_SIZE,
    ):
        """
        :param threadpool: The threadpool to use
        :param logger: The logger to use
        :param max_concurrent_tasks: The maximum number of tasks that can be pending at the same time
        """
        self._threadpool = threadpool
        self._logger = logger
        self._max_concurrent_tasks = max_concurrent_tasks
        self._futures: List[Future[Any]] = []
        self._lock = threading.Lock()
        self._most_recently_seen_exception: Optional[Exception] = None

    def prune_to_validate_has_reached_futures_limit(self) -> bool:
        self._prune_futures(self._futures)
        return len(self._futures) >= self._max_concurrent_tasks

    def submit(self, function: Callable[..., Any], *args: Any) -> None:
        self._futures.append(self._threadpool.submit(function, *args))

    def _prune_futures(self, futures: List[Future[Any]]) -> None:
        """
        Take a list in input and remove the futures that are completed. If a future has an exception, it'll raise and kill the stream
        operation.

        We are using a lock here as without it, the algorithm would not be thread safe
        """
        with self._lock:
            if len(futures) < self._max_concurrent_tasks:
                return

            for index in reversed(range(len(futures))):
                future = futures[index]

                if future.done():
                    # Only call future.exception() if the future is known to be done because it will block until the future is done.
                    # See https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Future.exception
                    optional_exception = future.exception()
                    if optional_exception:
                        # Exception handling should be done in the main thread. Hence, we only store the exception and expect the main
                        # thread to call raise_if_exception
                        self._most_recently_seen_exception = RuntimeError(f"Failed reading with error: {optional_exception}")
                    futures.pop(index)

    def shutdown(self) -> None:
        self._threadpool.shutdown(wait=False, cancel_futures=True)

    def is_done(self) -> bool:
        return all([f.done() for f in self._futures])

    def raise_if_exception(self) -> None:
        if self._most_recently_seen_exception:
            self._stop_and_raise_exception(self._most_recently_seen_exception)

    def check_for_errors_and_shutdown(self) -> None:
        """
        Check if any of the futures have an exception, and raise it if so. If all futures are done, shutdown the threadpool.
        If the futures are not done, raise an exception.
        :return:
        """
        self.raise_if_exception()

        exceptions_from_futures = [f for f in [future.exception() for future in self._futures] if f is not None]
        if exceptions_from_futures:
            exception = RuntimeError(f"Failed reading with errors: {exceptions_from_futures}")
            self._stop_and_raise_exception(exception)
        else:
            futures_not_done = [f for f in self._futures if not f.done()]
            if futures_not_done:
                exception = RuntimeError(f"Failed reading with futures not done: {futures_not_done}")
                self._stop_and_raise_exception(exception)
            else:
                self.shutdown()

    def _stop_and_raise_exception(self, exception: BaseException) -> None:
        self.shutdown()
        raise exception
