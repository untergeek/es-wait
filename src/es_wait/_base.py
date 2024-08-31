"""Base Waiter Class"""

import typing as t
from sys import version_info
import logging
from pprint import pformat
from time import sleep
from datetime import datetime, timezone
from .utils import indicator_generator

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger('es_wait.Waiter')


class Waiter:
    """Waiter Parent Class"""

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = 9,  # The delay between checks
        timeout: float = -1,  # How long is too long
    ) -> None:
        #: An :py:class:`Elasticsearch <elasticsearch.Elasticsearch>` client instance
        self.client = client
        #: The delay between checks for completion
        self.pause = pause
        #: The number of seconds before giving up. -1 means no timeout.
        self.timeout = timeout
        self.waitstr = 'for Waiter class to initialize'
        #: Only changes to True in certain circumstances
        self.do_health_report = False

    @property
    def now(self) -> datetime:
        """
        :getter: Returns the time 'now' in the UTC timezone
        :type: datetime
        """
        return datetime.now(timezone.utc)

    @property
    def check(self) -> bool:
        """
        This will be redefined by each child class

        :getter: Returns if the check was complete
        :type: bool
        """
        return False

    def empty_check(self, name: str) -> None:
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

    def prettystr(self, *args, **kwargs) -> str:
        """
        A (nearly) straight up wrapper for :py:meth:`pprint.pformat()
        <pprint.PrettyPrinter.pformat>`, except that we provide our own default values
        for `indent` (`2`) and `sort_dicts` (`False`). Primarily for debug logging and
        showing more readable dictionaries.

        'Return the formatted representation of object as a string. indent, width,
        depth, compact, sort_dicts and underscore_numbers are passed to the
        PrettyPrinter constructor as formatting parameters' (from pprint
        documentation).
        """
        defaults = [
            ('indent', 2),
            ('width', 80),
            ('depth', None),
            ('compact', False),
            ('sort_dicts', False),
        ]
        if version_info[0] >= 3 and version_info[1] >= 10:
            # underscore_numbers only works in 3.10 and up
            defaults.append(('underscore_numbers', False))
        kw = {}
        for tup in defaults:
            key, default = tup
            kw[key] = kwargs[key] if key in kwargs else default

        return f"\n{pformat(*args, **kw)}"  # newline in front so it's always clean

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
        and generate many log lines at INFO level showing whatever was found.

        :param frequency: The number of seconds between log reports on progress.
        """
        # Now with this mapped, we can perform the wait as indicated.
        start_time = self.now
        success = False
        logger.debug('Only logging every %s seconds', frequency)
        while True:
            elapsed = int((self.now - start_time).total_seconds())
            if elapsed == 0:
                loggit = False
            else:
                loggit = elapsed % frequency == 0  # Only frequency seconds
            response = self.check
            # Successfully completed task.
            if response:
                logger.debug('The wait %s is over.', self.waitstr)
                total = f'{(self.now - start_time).total_seconds():.2f}'
                logger.debug('Elapsed time: %s seconds', total)
                success = True
                break
            # Not success, and reached timeout (if defined)
            if (self.timeout != -1) and (elapsed >= self.timeout):
                msg = (
                    f'The {self.waitstr} did not complete within {self.timeout} '
                    f'seconds.'
                )
                logger.error(msg)
                break
            # Not timed out and not yet success, so we wait.
            if loggit:
                msg = (
                    f'The wait {self.waitstr} is ongoing. {elapsed} total seconds '
                    f'have elapsed. Pausing {self.pause} seconds between checks.'
                )
                logger.debug(msg)
            sleep(self.pause)  # Actual wait here

        if not success:
            msg = (
                f'The wait {self.waitstr} failed to complete in the timeout period of '
                f'{self.timeout} seconds'
            )
            logger.error(msg)
            if self.do_health_report:
                rpt = dict(self.client.health_report())
                if rpt['status'] != 'green':
                    logger.info('HEALTH REPORT: STATUS: %s', {rpt['status'].upper()})
                    inds = rpt['indicators']
                    for ind in inds:
                        if isinstance(ind, str):
                            if inds[ind]['status'] != 'green':
                                for line in indicator_generator(ind, inds[ind]):
                                    logger.info('HEALTH REPORT: %s', line)

            raise TimeoutError(msg)
