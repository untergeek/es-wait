"""ILM Phase and Step Check Waiters"""

import typing as t
import logging
from dotmap import DotMap  # type: ignore
from elasticsearch8.exceptions import NotFoundError
from ._base import Waiter
from .defaults import ILM
from .exceptions import IlmWaitError
from .utils import prettystr

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger(__name__)


class IndexLifecycle(Waiter):
    """ILM Step and Phase Parent Class"""

    # pylint: disable=R0913,R0917
    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = ILM.get('pause', 9.0),
        timeout: float = ILM.get('timeout', 630.0),
        max_exceptions: int = ILM.get('max_exceptions', 10),
        name: str = '',
    ) -> None:
        """
        Initializes the IndexLifecycle waiter.

        'pause' will check every 15 seconds by default
        'timeout' is 10 minutes, 30 seconds, which is a reflection of Elasticsearch's
        default ILM polling interval of 10 minutes.
        """
        super().__init__(
            client=client, pause=pause, timeout=timeout, max_exceptions=max_exceptions
        )
        #: The index name
        self.name = name
        self._ensure_not_none('name')

    def get_explain_data(self) -> t.Union[t.Dict, None]:
        """
        This method calls :py:meth:`ilm.explain_lifecycle()
        <elasticsearch.client.IlmClient.explain_lifecycle>` with :py:attr:`name` and
        returns the resulting response.
        """
        try:
            resp = dict(self.client.ilm.explain_lifecycle(index=self.name))
            logger.debug(f'ILM Explain response: {prettystr(resp)}')
        except NotFoundError as exc:
            msg = (
                f'Datastream/Index Name changed. {self.name} was not found. '
                f'This is likely due to the index name suddenly changing, as with '
                f'searchable snapshot mounts.'
            )
            logger.error(msg)
            raise exc  # re-raise the original. Just wanted to log here.
        # Proposed possible retry solution to name changes
        # Use an on_name_change callout to handle the name change
        # def get_explain_data(self, on_name_change: t.Optional[t.Callable] = None):
        #     try:
        #         resp = dict(self.client.ilm.explain_lifecycle(index=self.name))
        #     except NotFoundError as exc:
        #         if on_name_change:
        #             new_name = on_name_change(self.name)
        #             if new_name:
        #                 self.name = new_name
        #                 return self.get_explain_data()
        #         raise exc
        # The problem is that there's no way to communicate the name change
        # back to the caller. It's a bit of a mess.
        except Exception as err:
            msg = f'Unable to get ILM information for index {self.name}'
            logger.critical(msg)
            raise IlmWaitError(f'{msg}. Exception: {prettystr(err)}') from err
        retval = resp['indices'][self.name]
        return retval


class IlmPhase(IndexLifecycle):
    """
    ILM Phase class (child of class IndexLifecycle)

    It should be noted that the default ILM polling interval in Elasticsearch is 10
    minutes. Setting pause and timeout accordingly is a good idea.
    """

    # pylint: disable=R0913,R0917
    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = ILM.get('pause', 9.0),
        timeout: float = ILM.get('timeout', 630.0),
        max_exceptions: int = ILM.get('max_exceptions', 10),
        name: str = '',
        phase: str = '',
    ) -> None:
        super().__init__(
            client=client,
            pause=pause,
            timeout=timeout,
            max_exceptions=max_exceptions,
            name=name,
        )
        #: The target ILM phase
        self.phase = phase
        self._ensure_not_none('phase')
        self.waitstr = (
            f'for "{self.name}" to complete ILM transition to phase "{self.phase}"'
        )
        logger.debug(f'Waiting {self.waitstr}...')

    def check(self) -> bool:
        """
        Collect ILM explain data from :py:meth:`get_explain_data()`, and check for ILM
        phase change completion.  It will return ``True`` if the expected phase and the
        actually collected phase match. If phase is ``new``, it will return ``True`` if
        the collected phase is ``new`` or higher (``hot``, ``warm``, ``cold``,
        ``frozen``, ``delete``).

        Upstream callers need to try/catch any of :py:exc:`KeyError` (index name
        changed), :py:exc:`NotFoundError <elasticsearch.exceptions.NotFoundError>`, and
        :py:exc:`~.es_wait.exceptions.IlmWaitError`.

        We cannot not be responsible for retrying with a changed name as it's not in
        our scope as a "waiter"

        :getter: Returns if the check was complete
        :type: bool
        """
        self.too_many_exceptions()
        explain = DotMap(self.get_explain_data())
        if not explain:
            logger.warning('No ILM Explain data found.')
            self.exceptions_raised += 1
            return False
        logger.debug(f'ILM Explain data: {explain}')
        if not explain.phase:
            logger.warning('No ILM Phase found.')
            self.exceptions_raised += 1
            return False
        logger.info(f'ILM Phase {explain.phase} found.')
        if self.phase == 'new':
            logger.debug('Expecting ILM Phase new, or higher')
            if self.phase_by_num(explain.phase) >= self.phase_by_num(self.phase):
                return True
        else:
            logger.debug(f'Expecting ILM Phase {self.phase}')
        return bool(explain.phase == self.phase)

    def phase_by_num(self, phase: str) -> int:
        """Map a phase name to a phase number"""
        _ = {
            'undef': 0,
            'new': 1,
            'hot': 2,
            'warm': 3,
            'cold': 4,
            'frozen': 5,
            'delete': 6,
        }
        if phase in _:
            return _[phase]
        return 0  # Default to 0/undef if not found

    def phase_by_name(self, num: int) -> str:
        """Map a phase number to a phase name"""
        _ = {
            # 0: 'undef',
            1: 'new',
            2: 'hot',
            3: 'warm',
            4: 'cold',
            5: 'frozen',
            6: 'delete',
        }
        if num in _:
            return _[num]
        return 'undef'  # Default to 'undef' if not found


class IlmStep(IndexLifecycle):
    """
    ILM Step class (child of class IndexLifecycle)

    It should be noted that the default ILM polling interval in Elasticsearch is 10
    minutes. Setting pause and timeout accordingly is a good idea.
    """

    # pylint: disable=R0913,R0917
    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = ILM.get('pause', 9.0),
        timeout: float = ILM.get('timeout', 630.0),
        max_exceptions: int = ILM.get('max_exceptions', 10),
        name: str = '',
    ) -> None:
        super().__init__(
            client=client,
            pause=pause,
            timeout=timeout,
            max_exceptions=max_exceptions,
            name=name,
        )
        self.waitstr = f'for "{self.name}" to complete the current ILM step'

    def check(self) -> bool:
        """
        Collect ILM explain data from :py:meth:`get_explain_data()`, and check for ILM
        step completion.  It will return ``True`` if the step and action values are
        both ``complete``

        Upstream callers need to try/catch any of :py:exc:`KeyError` (index name
        changed), :py:exc:`NotFoundError <elasticsearch.exceptions.NotFoundError>`, and
        :py:exc:`~.es_wait.exceptions.IlmWaitError`.

        We cannot not be responsible for retrying with a changed name as it's not in
        our scope as a "waiter"

        :getter: Returns if the check was complete
        :type: bool
        """
        self.too_many_exceptions()
        explain = DotMap(action='no', step='no')  # Set defaults so the return works
        try:
            explain = DotMap(self.get_explain_data())
        except NotFoundError as err:
            if self.client.indices.exists(index=self.name):
                msg = (
                    f'NotFoundError encountered. However, index {self.name} has been '
                    f'confirmed to exist, so we continue to retry...'
                )
                logger.debug(msg)
            else:
                self.exceptions_raised += 1
                raise err
        return bool(explain.action == 'complete' and explain.step == 'complete')
