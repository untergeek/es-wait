"""Base Waiter Class."""

# pylint: disable=R0902,R0913,R0917,W0718
import typing as t
import logging
from time import sleep
from datetime import datetime, timezone
from elasticsearch8.exceptions import TransportError
from .debug import debug, begin_end
from .defaults import BASE
from .exceptions import (
    EsWaitException,
    EsWaitFatal,
    EsWaitTimeout,
    ExceptionCount,
    IlmWaitError,
)
from .utils import health_report, prettystr

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger('es_wait.Waiter')


class TimeTracker:
    """Track time for wait operations.

    Manages elapsed time and determines when to log progress based on frequency.

    Args:
        log_frequency (int): Seconds between log messages (default: 5).

    Attributes:
        log_frequency (int): Seconds between log messages.
        start_time (:py:class:`datetime.datetime`): Start time in UTC.
    """

    def __init__(self, log_frequency: int = 5) -> None:
        """Initialize the TimeTracker.

        Args:
            log_frequency (int): Seconds between log messages (default: 5).

        Example:
            >>> tracker = TimeTracker(log_frequency=5)
            >>> tracker.log_frequency
            5
        """
        debug.lv2('Initializing TimeTracker object...')
        self.log_frequency = log_frequency
        self.start_time = self.now
        debug.lv3('TimeTracker object initialized')

    @property
    def elapsed(self) -> float:
        """Return the elapsed time in seconds.

        Returns:
            float: Elapsed time since initialization in seconds.
        """
        return (self.now - self.start_time).total_seconds()

    @property
    def now(self) -> datetime:
        """Return the current time in UTC.

        Returns:
            :py:class:`datetime.datetime`: Current time in UTC.
        """
        return datetime.now(timezone.utc)

    @property
    def should_log(self) -> bool:
        """Check if a log message should be generated.

        Returns True if elapsed time is non-zero and a multiple of log_frequency.

        Returns:
            bool: True if a log message should be generated, False otherwise.

        Example:
            >>> tracker = TimeTracker(log_frequency=5)
            >>> tracker.elapsed = 5.0  # Simulate 5 seconds
            >>> tracker.should_log
            True
        """
        if int(self.elapsed) == 0:
            return False
        return int(self.elapsed) % self.log_frequency == 0


class Waiter:
    """Base class for waiting on Elasticsearch operations.

    Manages polling, timeouts, and exceptions for tasks like index relocation or
    snapshot completion.

    Args:
        client (:py:class:`elasticsearch8.Elasticsearch`): Elasticsearch client.
        pause (float): Seconds between checks (default: 9.0).
        timeout (float): Max wait time in seconds (default: 15.0, -1 for no timeout).
        max_exceptions (int): Max allowed exceptions (default: 10).

    Attributes:
        client (:py:class:`elasticsearch8.Elasticsearch`): Elasticsearch client.
        pause (float): Seconds between checks.
        timeout (float): Max wait time in seconds.
        max_exceptions (int): Max allowed exceptions.
        exceptions_raised (int): Number of exceptions raised.
        do_health_report (bool): If True, logs health report on failure.
        waitstr (str): Description of the wait operation.
    """

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = BASE.get('pause', 9.0),
        timeout: float = BASE.get('timeout', 15.0),
        max_exceptions: int = BASE.get('max_exceptions', 10),
    ) -> None:
        """Initialize the Waiter.

        Args:
            client (:py:class:`elasticsearch8.Elasticsearch`): Elasticsearch client.
            pause (float): Seconds between checks (default: 9.0).
            timeout (float): Max wait time in seconds (default: 15.0).
            max_exceptions (int): Max allowed exceptions (default: 10).

        Example:
            >>> from elasticsearch8 import Elasticsearch
            >>> client = Elasticsearch()
            >>> waiter = Waiter(client, pause=5.0, timeout=30.0)
            >>> waiter.pause
            5.0
        """
        debug.lv2('Initializing Waiter object...')
        self.client = client
        self.pause = pause
        self.timeout = timeout
        self.max_exceptions = max_exceptions
        self._exceptions = []
        self.exceptions_raised = 0
        self.waitstr = 'for Waiter class to initialize'
        self.do_health_report = False
        debug.lv3('Waiter object initialized')

    @property
    def exception_count_msg(self) -> str:
        """Return a message showing the number of exceptions raised.

        Returns:
            str: Message with current and max allowed exceptions.

        Example:
            >>> waiter = Waiter(client, max_exceptions=10)
            >>> waiter.exceptions_raised = 2
            >>> waiter.exception_count_msg
            '2 exceptions raised out of 10 allowed'
        """
        return (
            f'{self.exceptions_raised} exceptions raised out of '
            f'{self.max_exceptions} allowed'
        )

    @property
    def exceptions(self) -> list:
        """Return the list of exceptions raised during the wait.

        Returns:
            list: List of exceptions raised.
        """
        return self._exceptions

    def add_exception(self, value: Exception) -> None:
        """Append an exception to the exceptions list.

        Args:
            value (:py:class:`Exception`): Exception to add.
        """
        self._exceptions.append(value)

    @begin_end()
    def announce(self) -> None:
        """Log the start of the wait operation.

        Example:
            >>> waiter = Waiter(client)
            >>> waiter.waitstr = "test wait"
            >>> waiter.announce()  # Logs: "The wait test wait is starting..."
        """
        debug.lv1(f'The wait {self.waitstr} is starting...')

    def check(self) -> bool:
        """Check if the task is complete (to be overridden by subclasses).

        Returns:
            bool: True if the task is complete, False otherwise.
        """
        return False

    def _ensure_not_none(self, name: str) -> None:
        """Raise ValueError if an attribute is None.

        Args:
            name (str): Name of the attribute to check.

        Raises:
            ValueError: If the attribute is None.

        Example:
            >>> waiter = Waiter(client)
            >>> waiter.name = None
            >>> waiter._ensure_not_none("name")
            Traceback (most recent call last):
                ...
            ValueError: Keyword arg name cannot be None
        """
        if getattr(self, name) is None:
            msg = f'Keyword arg {name} cannot be None'
            logger.critical(msg)
            raise ValueError(msg)

    @begin_end()
    def too_many_exceptions(self) -> None:
        """Raise ExceptionCount if too many exceptions occurred.

        Raises:
            :py:class:`es_wait.exceptions.ExceptionCount`: If exceptions exceed max.

        Example:
            >>> waiter = Waiter(client, max_exceptions=1)
            >>> waiter.exceptions_raised = 2
            >>> waiter.too_many_exceptions()
            Traceback (most recent call last):
                ...
            ExceptionCount: Check for Waiter class to initialize has failed...
        """
        if self.exceptions_raised >= self.max_exceptions:
            msg = f'Check {self.waitstr} has failed, {self.exception_count_msg}'
            debug.lv3('Exiting method, raising exception')
            logger.error(msg)
            debug.lv5('Exception = ExceptionCount')
            raise ExceptionCount(msg, self.exceptions_raised, tuple(self.exceptions))

    @begin_end()
    def wait(self, frequency: int = 5) -> None:
        """Wait until the task completes or times out.

        Polls the task every `pause` seconds, logging progress every `frequency`
        seconds. Raises an exception on timeout or too many errors.

        Args:
            frequency (int): Seconds between progress logs (default: 5).

        Raises:
            :py:class:`es_wait.exceptions.EsWaitTimeout`: If task exceeds timeout.
            :py:class:`es_wait.exceptions.EsWaitFatal`: If a fatal error occurs.
            :py:class:`es_wait.exceptions.ExceptionCount`: If max_exceptions exceeded.

        Example:
            >>> from elasticsearch8 import Elasticsearch
            >>> client = Elasticsearch()
            >>> waiter = Waiter(client, pause=5.0, timeout=30.0)
            >>> try:
            ...     waiter.wait(frequency=10)
            ... except EsWaitTimeout as e:
            ...     print(f"Timed out after {e.elapsed} seconds")
        """
        tracker = TimeTracker(log_frequency=frequency)
        success = False
        debug.lv2(f'Only logging every {frequency} seconds')
        while True:
            try:
                debug.lv4('TRY: Checking for completion')
                response = self.check()
                debug.lv5(f'check() response: {response}')
            except ExceptionCount as err:
                debug.lv3('Exiting method, raising exception')
                logger.critical(f'{self.exception_count_msg} from {prettystr(err)}')
                raise EsWaitFatal(
                    self.exception_count_msg, tracker.elapsed, tuple(self.exceptions)
                ) from err
            except (EsWaitException, IlmWaitError) as err:
                msg = f'An error occurred: {prettystr(err)}'
                debug.lv3('Exiting method, raising exception')
                logger.critical(msg)
                raise EsWaitFatal(msg, tracker.elapsed, tuple(self.exceptions)) from err
            if response:
                debug.lv2(f'The wait {self.waitstr} is over.')
                total = f'{tracker.elapsed:.2f}'
                debug.lv3(f'Elapsed time: {total} seconds')
                success = True
                break
            if (self.timeout != -1) and (tracker.elapsed >= self.timeout):
                msg = (
                    f'The {self.waitstr} did not complete within {self.timeout} '
                    f'seconds.'
                )
                logger.error(msg)
                break
            if tracker.should_log:
                msg = (
                    f'The wait {self.waitstr} is ongoing. {tracker.elapsed} '
                    f'total seconds have elapsed. Pausing {self.pause} seconds '
                    f'between checks.'
                )
                debug.lv2(msg)
            sleep(self.pause)

        if not success:
            msg = (
                f'The wait {self.waitstr} failed to complete in the timeout '
                f'period of {self.timeout} seconds'
            )
            logger.error(msg)
            timeout = EsWaitTimeout(msg, tracker.elapsed, self.timeout)
            if self.do_health_report:
                try:
                    debug.lv4('TRY: Getting health report')
                    health_report(self.client.health_report())
                except TransportError as exc:
                    fail = f'Health report failed: {exc}'
                    logger.error(fail)
            debug.lv3('Exiting method, raising exception')
            debug.lv5(f'Exception = "{prettystr(timeout)}"')
            raise timeout
