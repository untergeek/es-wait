"""Base Waiter Class"""
import typing as t
import logging
from time import sleep
from datetime import datetime, timezone
from elasticsearch8 import Elasticsearch

class Waiter:
    """Class Definition"""
    ACTIONS = ['any', 'listed', 'actions']
    def __init__(
            self,
            client: Elasticsearch,
            action: t.Union[str, t.Sequence[str]] = None,
            pause: float = 9,    # The delay between checks
            timeout: float = -1, # How long is too long
        ) -> None:
        self.logger = logging.getLogger('es_wait.Base')
        self.client = client
        self.action = action
        self.pause = pause
        self.timeout = timeout
        self.checkid = 'check for Waiter class'

    @property
    def acceptable(self) -> t.Union[str, t.Sequence[str]]:
        """Return acceptable actions for this class"""
        return self.ACTIONS

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

    def wait_for_it(self) -> None:
        """Do the actual waiting"""
        # Now with this mapped, we can perform the wait as indicated.
        start_time = self.now
        result = False
        while True:
            elapsed = int((self.now - start_time).total_seconds())
            self.logger.debug('Elapsed time: %s seconds', elapsed)
            response = self.check
            if response:
                self.logger.debug('%s finished executing', self.checkid)
                result = True
                break
            # Not success, and reached timeout (if defined)
            if (self.timeout != -1) and (elapsed >= self.timeout):
                msg = f'The {self.checkid} did not complete within {self.timeout} seconds.'
                self.logger.error(msg)
                break
            # Not timed out and not yet success, so we wait.
            msg = (
                f'The {self.checkid} is not yet complete. {elapsed} total seconds have elapsed. '
                f'Waiting {self.pause} seconds before checking again.'
            )
            self.logger.debug(msg)
            sleep(self.pause)

        if not result:
            msg = (
                f'The {self.checkid} failed to complete in the '
                f'timeout period of {self.timeout} seconds'
            )
            self.logger.error(msg)
            raise TimeoutError(msg)
