"""Base Waiter Class"""

import typing as t
import logging
from time import sleep
from datetime import datetime, timezone

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch


class Waiter:
    """Class Definition"""

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = 9,  # The delay between checks
        timeout: float = -1,  # How long is too long
    ) -> None:
        self.logger = logging.getLogger('es_wait.Base')
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
            self.logger.critical(msg)
            raise ValueError(msg)

    def setup(self) -> None:
        """Setup the waiter"""

    def wait_for_it(self, frequency: int = 5) -> None:
        """Do the actual waiting"""
        # Now with this mapped, we can perform the wait as indicated.
        start_time = self.now
        success = False
        self.logger.debug('Only logging every %s seconds', frequency)
        while True:
            elapsed = int((self.now - start_time).total_seconds())
            if elapsed == 0:
                loggit = False
            else:
                loggit = elapsed % frequency == 0  # Only frequency seconds
            response = self.check
            # Successfully completed task.
            if response:
                self.logger.debug('%s finished executing', self.checkid)
                total = f'{(self.now - start_time).total_seconds():.2f}'
                self.logger.debug('Elapsed time: %s seconds', total)
                success = True
                break
            # Not success, and reached timeout (if defined)
            if (self.timeout != -1) and (elapsed >= self.timeout):
                msg = (
                    f'The {self.checkid} did not complete within {self.timeout} '
                    f'seconds.'
                )
                self.logger.error(msg)
                break
            # Not timed out and not yet success, so we wait.
            if loggit:
                msg = (
                    f'The {self.checkid} is not yet complete. {elapsed} total seconds '
                    f'have elapsed. Pausing {self.pause} seconds between checks.'
                )
                self.logger.debug(msg)
            sleep(self.pause)  # Actual wait here

        if not success:
            msg = (
                f'The {self.checkid} failed to complete in the timeout period of '
                f'{self.timeout} seconds'
            )
            self.logger.error(msg)
            raise TimeoutError(msg)