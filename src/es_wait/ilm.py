"""ILM Phase and Step Check Waiters"""

# pylint: disable=R0902,R0913,R0917,W0718
import typing as t
import logging
from dotmap import DotMap  # type: ignore
import tiered_debug as debug
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

    @property
    def explain(self) -> str:
        """
        :getter: Returns the current ilm explain data for the index
        :type: str
        """
        return DotMap(self.get_explain_data())

    @property
    def phase_complete(self) -> bool:
        """
        :getter: Returns True if both action and step for the phase are complete
        :type: bool
        """
        return bool(
            self.explain.action == 'complete' and self.explain.step == 'complete'
        )

    def get_explain_data(self) -> t.Union[t.Dict, None]:
        """
        This method calls :py:meth:`ilm.explain_lifecycle()
        <elasticsearch.client.IlmClient.explain_lifecycle>` with :py:attr:`name` and
        returns the resulting response.
        """
        debug.lv2('Starting method...')
        try:
            debug.lv4('TRY: Getting ILM explain data...')
            resp = dict(self.client.ilm.explain_lifecycle(index=self.name))
            debug.lv5(f'ILM Explain response: {prettystr(resp)}')
        except NotFoundError as exc:
            msg = (
                f'Datastream/Index Name changed. {self.name} was not found. '
                f'This is likely due to the index name suddenly changing, as with '
                f'searchable snapshot mounts.'
            )
            debug.lv3('Exiting method, raising exception')
            debug.lv5(f'Exception = {prettystr(exc)}')
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
        debug.lv3('Exiting method, returning value')
        debug.lv5(f'Value = {prettystr(retval)}')
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
        debug.lv2('Initializing IlmPhase object...')
        #: The target ILM phase
        self.phase = phase
        self._ensure_not_none('phase')
        self.waitstr = (
            f'for "{self.name}" to complete ILM transition to phase "{self.phase}"'
        )
        self.announce()
        self.stuck_count = 0
        self.advanced = False
        debug.lv3('IlmPhase object initialized')

    @property
    def phase_gte(self) -> bool:
        """
        :getter: Returns True if the current phase meets or exceeds the target phase
        :type: bool
        """
        return bool(
            self.phase_by_num(self.explain.phase) >= self.phase_by_num(self.phase)
        )

    @property
    def phase_lt(self) -> bool:
        """
        :getter: Returns True if the current phase is less the target phase
        :type: bool
        """
        return bool(
            self.phase_by_num(self.explain.phase) < self.phase_by_num(self.phase)
        )

    def has_explain(self) -> bool:
        """Check if the explain data is present
        :returns: boolean of "The explain data is present"
        """
        if not self.explain:
            logger.warning('No ILM Explain data found.')
            self.exceptions_raised += 1
            debug.lv3('Exiting method, returning value')
            debug.lv5('Value = False')
            return False
        debug.lv5(f'ILM Explain data: {self.explain.toDict()}')
        if not self.explain.phase:
            logger.warning('No ILM Phase found.')
            self.exceptions_raised += 1
            debug.lv3('Exiting method, returning value')
            debug.lv5('Value = False')
            return False
        debug.lv3('Exiting method, returning value')
        debug.lv5('Value = True')
        return True

    def reached_phase(self) -> bool:
        """Check if the phase is what we expect
        :returns: boolean of "The phase reached its target"
        """
        debug.lv2('Starting method...')
        if self.phase_gte and self.phase == 'new':
            debug.lv2('ILM Phase: new is complete')
            debug.lv3('Exiting method, returning value')
            debug.lv5('Value = True')
            return True
        if self.phase_lt and self.phase_complete:
            self.stuck_count += 1
            logger.info(
                f'ILM Phase: {self.explain.phase} is complete, '
                f'but expecting {self.phase}. Seen {self.stuck_count} times'
            )
            debug.lv3('Exiting method, returning value')
            debug.lv5('Value = False')
            return False
        if self.phase_lt:
            debug.lv2(f'ILM has not yet reached phase {self.phase}')
            debug.lv3('Exiting method, returning value')
            debug.lv5('Value = False')
            return False
        debug.lv3('Exiting method, returning value')
        debug.lv5('Value = True')
        return True  # The phase is now gte to the target phase

    def phase_stuck(self, max_stuck_count: int = 3) -> bool:
        """Check if the phase is stuck
        :param int max_stuck_count: The maximum number of times the phase can be stuck
            before returning raising an exception. Default is 3.
        :type max_stuck_count: int
        :returns: boolean of "The phase is stuck"
        """
        debug.lv2('Starting method...')
        if self.stuck_count >= max_stuck_count:
            if self.advanced:
                msg = (
                    f'ILM phase {self.phase} was stuck, but was advanced. '
                    f'Even after advancing, the phase is still stuck.'
                )
                logger.error(msg)
                raise IlmWaitError(msg)
            msg = (
                f'Expecting ILM phase {self.phase}, but current phase is '
                f'{self.explain.phase}, which is complete. ILM phase advance '
                f'does not appear to be happening after {self.stuck_count} '
                f'iterations (max retries {max_stuck_count}). Triggering an ILM '
                f'phase advance to {self.phase}'
            )
            logger.warning(msg)
            # Trigger advance to self.phase
            curr = {'phase': self.explain.phase, 'action': 'complete'}
            curr['name'] = 'complete'  # So odd that it's step in the output
            target = {'phase': self.phase, 'action': 'complete'}
            target['name'] = 'complete'  # But it's name in the API
            self.client.ilm.move_to_step(
                index=self.name, current_step=curr, next_step=target
            )
            self.advanced = True
            self.stuck_count = 0  # Reset the stuck count
            self.exceptions_raised = 0  # Reset the exceptions_raised count
            debug.lv3('Exiting method, returning value')
            debug.lv5('Value = True')
            return True  # The phase was stuck
        debug.lv3('Exiting method, returning value')
        debug.lv5('Value = False')
        return False  # The phase was not stuck

    def check(self, max_stuck_count: int = 3) -> bool:
        """Check the ILM phase transition

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

        :param int max_stuck_count: The maximum number of times the phase can be stuck
            before returning raising an exception. Default is 3.
        :type max_stuck_count: int
        :returns: Returns if the check was complete
        :rtype: bool
        """
        debug.lv2('Starting method...')
        self.too_many_exceptions()
        if not self.has_explain():
            return False
        logger.info(f'Current ILM Phase: {self.explain.phase}')
        debug.lv2(f'Expecting ILM Phase: {self.phase}')
        if self.phase_stuck(max_stuck_count):
            # The phase was stuck, and we triggered an ILM advance
            return False
        debug.lv2('ILM Phase not stuck.')
        retval = self.reached_phase()
        debug.lv3('Exiting method, returning value')
        debug.lv5(f'Value = {retval}')
        return retval

    def phase_by_num(self, phase: str) -> int:
        """Map a phase name to a phase number"""
        debug.lv2('Starting method...')
        _ = {
            'undef': 0,
            'new': 1,
            'hot': 2,
            'warm': 3,
            'cold': 4,
            'frozen': 5,
            'delete': 6,
        }
        retval = _.get(phase, 0)  # Default to 0/undef if not found
        debug.lv3('Exiting method, returning value')
        debug.lv5(f'Value = {retval}')
        return retval

    def phase_by_name(self, num: int) -> str:
        """Map a phase number to a phase name"""
        debug.lv2('Starting method...')
        _ = {
            # 0: 'undef',
            1: 'new',
            2: 'hot',
            3: 'warm',
            4: 'cold',
            5: 'frozen',
            6: 'delete',
        }
        retval = _.get(num, 'undef')  # Default to undef if not found
        debug.lv3('Exiting method, returning value')
        debug.lv5(f'Value = {retval}')
        return retval


class IlmStep(IndexLifecycle):
    """
    ILM Step class (child of class IndexLifecycle)

    It should be noted that the default ILM polling interval in Elasticsearch is 10
    minutes. Setting pause and timeout accordingly is a good idea.
    """

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
        debug.lv2('Initializing IlmStep object...')
        self.waitstr = f'for "{self.name}" to complete the current ILM step'
        self.announce()

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
        if not self.client.indices.exists(index=self.name):
            self.exceptions_raised += 1
            self.add_exception(IlmWaitError(f'Index {self.name} not found.'))
            debug.lv1(f'Index {self.name} not found.')
            retval = False
        else:
            retval = self.phase_complete
        debug.lv3('Exiting method, returning value')
        debug.lv5(f'Value = {retval}')
        return retval
