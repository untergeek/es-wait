"""Base Waiter Class"""

# pylint: disable=R0902,R0913,R0917,W0718
import typing as t
import logging
from time import sleep
from datetime import datetime, timezone
import tiered_debug as debug
from elasticsearch8.exceptions import TransportError
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
    """Track time for the Waiter class"""

    def __init__(self, log_frequency: int = 5) -> None:
        debug.lv2('Initializing TimeTracker object...')
        self.log_frequency = log_frequency
        self.start_time = self.now
        debug.lv3('TimeTracker object initialized')

    @property
    def elapsed(self) -> float:
        """
        :getter: Returns the elapsed time in seconds
        :type: float
        """
        return (self.now - self.start_time).total_seconds()

    @property
    def now(self) -> datetime:
        """
        :getter: Returns the time 'now' in the UTC timezone
        :type: datetime
        """
        return datetime.now(timezone.utc)

    @property
    def should_log(self) -> bool:
        """
        Determine if a log message should be generated based on the elapsed time
        and frequency. If the elapsed time is 0, then no log message will be
        generated.

        :rtype: bool
        :return: Whether a log message should be generated.
        """
        if int(self.elapsed) == 0:
            return False
        return int(self.elapsed) % self.log_frequency == 0  # Only frequency seconds


class Waiter:
    """Waiter Parent Class"""

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = BASE.get('pause', 9.0),  # The delay between checks
        timeout: float = BASE.get('timeout', 15.0),  # How long is too long
        max_exceptions: int = BASE.get(
            'max_exceptions', 10
        ),  # The maximum number of exceptions to allow
    ) -> None:
        debug.lv2('Initializing Waiter object...')
        #: An :py:class:`Elasticsearch <elasticsearch.Elasticsearch>` client instance
        self.client = client
        #: The delay between checks for completion
        self.pause = pause
        #: The number of seconds before giving up. -1 means no timeout.
        self.timeout = timeout
        #: The maximum number of exceptions to allow
        self.max_exceptions = max_exceptions
        #: A list of exceptions raised during the wait
        self._exceptions = []
        #: The number of exceptions raised
        self.exceptions_raised = 0
        self.waitstr = 'for Waiter class to initialize'
        #: Only changes to True in certain circumstances
        self.do_health_report = False
        debug.lv3('Waiter object initialized')

    @property
    def exception_count_msg(self) -> str:
        """
        This property returns a messgage showing the current number of exceptions
        raised and the maximum number of exceptions allowed.

        :getter: Returns 'X exceptions raised out of Y allowed'
        :type: str
        """
        return (
            f'{self.exceptions_raised} exceptions raised out of '
            f'{self.max_exceptions} allowed'
        )

    @property
    def exceptions(self) -> list:
        """
        This property returns a list of exceptions raised during the wait.

        :getter: Returns a list of exceptions raised
        :type: list
        """
        return self._exceptions

    def add_exception(self, value: Exception) -> None:
        """
        This method appends `value` the exceptions list.

        :param value: An exception to add
        :type value: Exception
        """
        self._exceptions.append(value)

    def announce(self) -> None:
        """
        This method is called when the Waiter class is initialized. It logs a
        level 1 debug message using :py:attr:`waitstr`.
        """
        debug.lv2('Starting method...')
        debug.lv1(f'The wait {self.waitstr} is starting...')
        debug.lv3('Exiting method')

    def check(self) -> bool:
        """
        This will be redefined by each child class

        :getter: Returns if the check was complete
        :type: bool
        """
        return False

    def _ensure_not_none(self, name: str) -> None:
        """
        Raise a :py:exc:`ValueError` if the instance attribute `name` is None. This
        method literally just checks:

          .. code-block:: python

             if getattr(self, name) is None:

        :param name: The name of an instance attribute.
        """
        if getattr(self, name) is None:
            msg = f'Keyword arg {name} cannot be None'
            logger.critical(msg)
            raise ValueError(msg)

    def too_many_exceptions(self) -> None:
        """
        If the number of exceptions raised is greater than or equal to the maximum
        number of exceptions allowed, then a :py:exc:`ExceptionCount` will be raised.
        """
        debug.lv2('Stating method...')
        if self.exceptions_raised >= self.max_exceptions:
            msg = f'Check {self.waitstr} has failed, {self.exception_count_msg}'
            debug.lv3('Exiting method, raising exception')
            logger.error(msg)
            debug.lv5('Exception = ExceptionCount')
            raise ExceptionCount(msg, self.exceptions_raised, tuple(self.exceptions))
        debug.lv3('Exiting method')

    def wait(self, frequency: int = 5) -> None:
        """
        This method is where the actual waiting occurs. Depending on what `frequency`
        is set to, you should see `non-DEBUG` level logs no more than every `frequency`
        seconds.

        If :py:attr:`timeout` has been reached without :py:meth:`check` returning as
        ``True``, then a :py:exc:`TimeoutError` will be raised.

        If :py:meth:`check` returns ``False``, then the method will wait
        :py:attr:`pause` seconds before calling :py:meth:`check` again.

        Elapsed time will be logged every `frequency` seconds, when :py:meth:`check` is
        ``True``, or when :py:attr:`timeout` is reached.

        If :py:attr:`do_health_report` is ``True``, then call
        :py:meth:`client.health_report() <elasticsearch.client.health_report>`
        and pass the results to
        :py:func:`utils.health_report() <es_wait.utils.health_report>`.

        :param frequency: The number of seconds between log reports on progress.
        """
        debug.lv2('Starting method...')
        # Now with this mapped, we can perform the wait as indicated.
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
            except (
                EsWaitException,
                IlmWaitError,
            ) as err:  # Catch any other local Exceptions
                msg = f'An error occurred: {prettystr(err)}'
                debug.lv3('Exiting method, raising exception')
                logger.critical(msg)
                raise EsWaitFatal(msg, tracker.elapsed, tuple(self.exceptions)) from err
            # Successfully completed task.
            if response:
                debug.lv2(f'The wait {self.waitstr} is over.')
                total = f'{tracker.elapsed:.2f}'
                debug.lv3(f'Elapsed time: {total} seconds')
                success = True
                break
            # Not success, and reached timeout (if defined)
            if (self.timeout != -1) and (tracker.elapsed >= self.timeout):
                msg = (
                    f'The {self.waitstr} did not complete within {self.timeout} '
                    f'seconds.'
                )
                logger.error(msg)
                break
            # Not timed out and not yet success, so we wait.
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
        debug.lv3('Exiting method')
