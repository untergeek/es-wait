"""Base Waiter Class"""

import typing as t
import logging
from pprint import pformat
from time import sleep
from datetime import datetime, timezone

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger('es_wait.Waiter')


class Waiter:
    """Class Definition"""

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = 9,  # The delay between checks
        timeout: float = -1,  # How long is too long
    ) -> None:
        self.client = client
        self.pause = pause
        self.timeout = timeout
        self.checkid = 'check for Waiter class'

    @property
    def now(self) -> datetime:
        """Return the 'now' datetime"""
        return datetime.now(timezone.utc)

    @property
    def check(self) -> bool:
        """This will need to be redefined by each child class"""
        return False

    def empty_check(self, name: str) -> None:
        """Ensure that no empty values sneak through"""
        if getattr(self, name) is None:
            msg = f'Keyword arg {name} cannot be None'
            logger.critical(msg)
            raise ValueError(msg)

    def prettystr(self, *args, **kwargs) -> str:
        """
        A (nearly) straight up wrapper for pprint.pformat, except that we provide our
        own default values for 'indent' (2) and 'sort_dicts' (False). Primarily for
        debug logging and showing more readable dictionaries.

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
            ('underscore_numbers', False),
        ]
        kw = {}
        for tup in defaults:
            key, default = tup
            kw[key] = kwargs[key] if key in kwargs else default

        return f"\n{pformat(*args, **kw)}"  # newline in front so it's always clean

    def setup(self) -> None:
        """Setup the waiter"""

    def wait_for_it(self, frequency: int = 5) -> None:
        """Do the actual waiting"""
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
                logger.debug('%s finished executing', self.checkid)
                total = f'{(self.now - start_time).total_seconds():.2f}'
                logger.debug('Elapsed time: %s seconds', total)
                success = True
                break
            # Not success, and reached timeout (if defined)
            if (self.timeout != -1) and (elapsed >= self.timeout):
                msg = (
                    f'The {self.checkid} did not complete within {self.timeout} '
                    f'seconds.'
                )
                logger.error(msg)
                break
            # Not timed out and not yet success, so we wait.
            if loggit:
                msg = (
                    f'The {self.checkid} is not yet complete. {elapsed} total seconds '
                    f'have elapsed. Pausing {self.pause} seconds between checks.'
                )
                logger.debug(msg)
            sleep(self.pause)  # Actual wait here

        if not success:
            msg = (
                f'The {self.checkid} failed to complete in the timeout period of '
                f'{self.timeout} seconds'
            )
            logger.error(msg)
            raise TimeoutError(msg)
