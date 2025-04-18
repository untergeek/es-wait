"""ILM Phase and Step Check Waiters."""

# pylint: disable=R0913,R0917
import typing as t
import logging
from dotmap import DotMap
from elasticsearch8.exceptions import NotFoundError
from ._base import Waiter
from .debug import debug, begin_end
from .defaults import ILM
from .exceptions import IlmWaitError
from .utils import prettystr

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger(__name__)


class IndexLifecycle(Waiter):
    """Base class for ILM phase and step waiters.

    Provides common functionality for waiting on ILM operations.

    Args:
        client (:py:class:`elasticsearch8.Elasticsearch`): Elasticsearch client.
        pause (float): Seconds between checks (default: 9.0).
        timeout (float): Max wait time in seconds (default: 630.0).
        max_exceptions (int): Max allowed exceptions (default: 10).
        name (str): Index name (default: '').

    Attributes:
        name (str): Index name.
        waitstr (str): Description of the wait operation.

    Raises:
        ValueError: If `name` is empty.

    Example:
        >>> from elasticsearch8 import Elasticsearch
        >>> client = Elasticsearch()
        >>> ilm = IndexLifecycle(client, name="my-index")
        >>> ilm.wait()
    """

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = ILM.get('pause', 9.0),
        timeout: float = ILM.get('timeout', 630.0),
        max_exceptions: int = ILM.get('max_exceptions', 10),
        name: str = '',
    ) -> None:
        """Initialize the IndexLifecycle waiter.

        Args:
            client (:py:class:`elasticsearch8.Elasticsearch`): Elasticsearch client.
            pause (float): Seconds between checks (default: 9.0).
            timeout (float): Max wait time in seconds (default: 630.0).
            max_exceptions (int): Max allowed exceptions (default: 10).
            name (str): Index name (default: '').

        Raises:
            ValueError: If `name` is empty.

        Example:
            >>> from elasticsearch8 import Elasticsearch
            >>> client = Elasticsearch()
            >>> ilm = IndexLifecycle(client, name="my-index")
            >>> ilm.name
            'my-index'
        """
        super().__init__(
            client=client, pause=pause, timeout=timeout, max_exceptions=max_exceptions
        )
        self.name = name
        self._ensure_not_none('name')

    def __repr__(self) -> str:
        """Return a string representation of the IndexLifecycle instance.

        Returns:
            str: String representation including name, waitstr, and pause.

        Example:
            >>> ilm = IndexLifecycle(client, name="my-index", pause=9.0)
            >>> repr(ilm)
            'IndexLifecycle(name="my-index", waitstr="for Waiter class to initialize",
            pause=9.0)'
        """
        parts = [
            f"name={self.name!r}",
            f"waitstr={self.waitstr!r}",
            f"pause={self.pause}",
        ]
        return f"{self.__class__.__name__}({', '.join(parts)})"

    @property
    def explain(self) -> str:
        """Get the current ILM explain data for the index.

        Returns:
            str: ILM explain data as a DotMap object.
        """
        return DotMap(self.get_explain_data())

    @property
    def phase_complete(self) -> bool:
        """Check if both action and step for the phase are complete.

        Returns:
            bool: True if both action and step are complete, False otherwise.
        """
        return bool(
            self.explain.action == 'complete' and self.explain.step == 'complete'
        )

    @begin_end()
    def get_explain_data(self) -> t.Union[t.Dict, None]:
        """Get ILM explain data for the index.

        Calls the ilm.explain_lifecycle API to retrieve ILM status.

        Returns:
            Union[Dict, None]: ILM explain data or None if not found.

        Raises:
            :py:class:`elasticsearch8.exceptions.NotFoundError`: If index not found.
            :py:class:`es_wait.exceptions.IlmWaitError`: If ILM data cannot be
                retrieved.

        Example:
            >>> ilm = IndexLifecycle(client, name="my-index")
            >>> data = ilm.get_explain_data()
            >>> isinstance(data, dict)
            True
        """
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
            raise exc
        except Exception as err:
            msg = f'Unable to get ILM information for index {self.name}'
            logger.critical(msg)
            raise IlmWaitError(f'{msg}. Exception: {prettystr(err)}') from err
        retval = resp['indices'][self.name]
        debug.lv5(f'Return value = {prettystr(retval)}')
        return retval


class IlmPhase(IndexLifecycle):
    """Wait for an ILM phase transition to complete.

    Polls ILM explain data to check if the index has reached the target phase.

    Args:
        client (:py:class:`elasticsearch8.Elasticsearch`): Elasticsearch client.
        pause (float): Seconds between checks (default: 9.0).
        timeout (float): Max wait time in seconds (default: 630.0).
        max_exceptions (int): Max allowed exceptions (default: 10).
        name (str): Index name (default: '').
        phase (str): Target ILM phase (default: '').

    Attributes:
        name (str): Index name.
        phase (str): Target ILM phase.
        stuck_count (int): Number of times phase was stuck.
        advanced (bool): If True, phase was advanced manually.
        waitstr (str): Description of the wait operation.

    Raises:
        ValueError: If `name` or `phase` is empty.

    Example:
        >>> from elasticsearch8 import Elasticsearch
        >>> client = Elasticsearch()
        >>> ilm = IlmPhase(client, name="my-index", phase="warm")
        >>> ilm.wait()
    """

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = ILM.get('pause', 9.0),
        timeout: float = ILM.get('timeout', 630.0),
        max_exceptions: int = ILM.get('max_exceptions', 10),
        name: str = '',
        phase: str = '',
    ) -> None:
        """Initialize the IlmPhase waiter.

        Args:
            client (:py:class:`elasticsearch8.Elasticsearch`): Elasticsearch client.
            pause (float): Seconds between checks (default: 9.0).
            timeout (float): Max wait time in seconds (default: 630.0).
            max_exceptions (int): Max allowed exceptions (default: 10).
            name (str): Index name (default: '').
            phase (str): Target ILM phase (default: '').

        Raises:
            ValueError: If `name` or `phase` is empty.

        Example:
            >>> from elasticsearch8 import Elasticsearch
            >>> client = Elasticsearch()
            >>> ilm = IlmPhase(client, name="my-index", phase="warm")
            >>> ilm.phase
            'warm'
        """
        super().__init__(
            client=client,
            pause=pause,
            timeout=timeout,
            max_exceptions=max_exceptions,
            name=name,
        )
        debug.lv2('Initializing IlmPhase object...')
        self.phase = phase
        self._ensure_not_none('phase')
        self.waitstr = (
            f'for "{self.name}" to complete ILM transition to phase "{self.phase}"'
        )
        self.announce()
        self.stuck_count = 0
        self.advanced = False
        debug.lv3('IlmPhase object initialized')

    def __repr__(self) -> str:
        """Return a string representation of the IlmPhase instance.

        Returns:
            str: String representation including name, phase, stuck_count,
                advanced, waitstr, and pause.

        Example:
            >>> ilm = IlmPhase(client, name="my-index", phase="warm", pause=9.0)
            >>> repr(ilm)
            'IlmPhase(name="my-index", phase="warm", stuck_count=0, advanced=False,
            waitstr="for \"my-index\" to complete ILM transition to phase \"warm\"",
            pause=9.0)'
        """
        parts = [
            f"name={self.name!r}",
            f"phase={self.phase!r}",
            f"stuck_count={self.stuck_count}",
            f"advanced={self.advanced}",
            f"waitstr={self.waitstr!r}",
            f"pause={self.pause}",
        ]
        return f"{self.__class__.__name__}({', '.join(parts)})"

    @property
    def phase_gte(self) -> bool:
        """Check if current phase meets or exceeds the target phase.

        Returns:
            bool: True if current phase is at or beyond target, False otherwise.
        """
        return bool(
            self.phase_by_num(self.explain.phase) >= self.phase_by_num(self.phase)
        )

    @property
    def phase_lt(self) -> bool:
        """Check if current phase is less than the target phase.

        Returns:
            bool: True if current phase is before target, False otherwise.
        """
        return bool(
            self.phase_by_num(self.explain.phase) < self.phase_by_num(self.phase)
        )

    @begin_end()
    def has_explain(self) -> bool:
        """Check if ILM explain data is present.

        Returns:
            bool: True if explain data is present, False otherwise.

        Example:
            >>> ilm = IlmPhase(client, name="my-index", phase="warm")
            >>> ilm.has_explain()  # Checks for explain data
            True
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
        debug.lv5('Return value = True')
        return True

    @begin_end()
    def reached_phase(self) -> bool:
        """Check if the target phase has been reached.

        Returns:
            bool: True if target phase is reached, False otherwise.

        Example:
            >>> ilm = IlmPhase(client, name="my-index", phase="warm")
            >>> ilm.reached_phase()  # Checks if phase is warm or beyond
            False
        """
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
        debug.lv5('Return value = True')
        return True

    @begin_end()
    def phase_stuck(self, max_stuck_count: int = 3) -> bool:
        """Check if the ILM phase is stuck.

        Triggers an ILM phase advance if stuck for too long.

        Args:
            max_stuck_count (int): Max times phase can be stuck (default: 3).

        Returns:
            bool: True if phase is stuck, False otherwise.

        Raises:
            :py:class:`es_wait.exceptions.IlmWaitError`: If phase remains stuck
                after advancing.

        Example:
            >>> ilm = IlmPhase(client, name="my-index", phase="warm")
            >>> ilm.stuck_count = 3
            >>> ilm.phase_stuck(max_stuck_count=3)  # Triggers phase advance
            True
        """
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
            curr = {'phase': self.explain.phase, 'action': 'complete'}
            curr['name'] = 'complete'
            target = {'phase': self.phase, 'action': 'complete'}
            target['name'] = 'complete'
            self.client.ilm.move_to_step(
                index=self.name, current_step=curr, next_step=target
            )
            self.advanced = True
            self.stuck_count = 0
            self.exceptions_raised = 0
            debug.lv3('Exiting method, returning value')
            debug.lv5('Value = True')
            return True
        debug.lv5('Return value = False')
        return False

    @begin_end()
    def check(self, max_stuck_count: int = 3) -> bool:
        """Check if the ILM phase transition is complete.

        Polls ILM explain data to verify if the target phase is reached.

        Args:
            max_stuck_count (int): Max times phase can be stuck (default: 3).

        Returns:
            bool: True if target phase is reached, False otherwise.

        Raises:
            :py:class:`es_wait.exceptions.IlmWaitError`: If phase remains stuck.
            :py:class:`elasticsearch8.exceptions.NotFoundError`: If index not found.
            KeyError: If explain data lacks required keys.

        Example:
            >>> from elasticsearch8 import Elasticsearch
            >>> client = Elasticsearch()
            >>> ilm = IlmPhase(client, name="my-index", phase="warm")
            >>> ilm.check()  # Returns True if phase is warm or beyond
            False
        """
        self.too_many_exceptions()
        if not self.has_explain():
            return False
        logger.info(f'Current ILM Phase: {self.explain.phase}')
        debug.lv2(f'Expecting ILM Phase: {self.phase}')
        if self.phase_stuck(max_stuck_count):
            return False
        debug.lv2('ILM Phase not stuck.')
        retval = self.reached_phase()
        debug.lv5(f'Return value = {retval}')
        return retval

    @begin_end()
    def phase_by_num(self, phase: str) -> int:
        """Map a phase name to a phase number.

        Args:
            phase (str): ILM phase name.

        Returns:
            int: Phase number (0 for undefined).

        Example:
            >>> ilm = IlmPhase(client, name="my-index", phase="warm")
            >>> ilm.phase_by_num("warm")
            3
        """
        _ = {
            'undef': 0,
            'new': 1,
            'hot': 2,
            'warm': 3,
            'cold': 4,
            'frozen': 5,
            'delete': 6,
        }
        retval = _.get(phase, 0)
        debug.lv5(f'Return value = {retval}')
        return retval

    @begin_end()
    def phase_by_name(self, num: int) -> str:
        """Map a phase number to a phase name.

        Args:
            num (int): Phase number.

        Returns:
            str: Phase name ('undef' for undefined).

        Example:
            >>> ilm = IlmPhase(client, name="my-index", phase="warm")
            >>> ilm.phase_by_name(3)
            'warm'
        """
        debug.lv2('Starting method...')
        _ = {
            1: 'new',
            2: 'hot',
            3: 'warm',
            4: 'cold',
            5: 'frozen',
            6: 'delete',
        }
        retval = _.get(num, 'undef')
        debug.lv5(f'Return value = {retval}')
        return retval


class IlmStep(IndexLifecycle):
    """Wait for the current ILM step to complete.

    Polls ILM explain data to check if the current step and action are complete.

    Args:
        client (:py:class:`elasticsearch8.Elasticsearch`): Elasticsearch client.
        pause (float): Seconds between checks (default: 9.0).
        timeout (float): Max wait time in seconds (default: 630.0).
        max_exceptions (int): Max allowed exceptions (default: 10).
        name (str): Index name (default: '').

    Attributes:
        name (str): Index name.
        waitstr (str): Description of the wait operation.

    Raises:
        ValueError: If `name` is empty.

    Example:
        >>> from elasticsearch8 import Elasticsearch
        >>> client = Elasticsearch()
        >>> ilm = IlmStep(client, name="my-index")
        >>> ilm.wait()
    """

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = ILM.get('pause', 9.0),
        timeout: float = ILM.get('timeout', 630.0),
        max_exceptions: int = ILM.get('max_exceptions', 10),
        name: str = '',
    ) -> None:
        """Initialize the IlmStep waiter.

        Args:
            client (:py:class:`elasticsearch8.Elasticsearch`): Elasticsearch client.
            pause (float): Seconds between checks (default: 9.0).
            timeout (float): Max wait time in seconds (default: 630.0).
            max_exceptions (int): Max allowed exceptions (default: 10).
            name (str): Index name (default: '').

        Raises:
            ValueError: If `name` is empty.

        Example:
            >>> from elasticsearch8 import Elasticsearch
            >>> client = Elasticsearch()
            >>> ilm = IlmStep(client, name="my-index")
            >>> ilm.name
            'my-index'
        """
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

    def __repr__(self) -> str:
        """Return a string representation of the IlmStep instance.

        Returns:
            str: String representation including name, waitstr, and pause.

        Example:
            >>> ilm = IlmStep(client, name="my-index", pause=9.0)
            >>> repr(ilm)
            'IlmStep(name="my-index", waitstr="for \"my-index\" to complete the
            current ILM step", pause=9.0)'
        """
        parts = [
            f"name={self.name!r}",
            f"waitstr={self.waitstr!r}",
            f"pause={self.pause}",
        ]
        return f"{self.__class__.__name__}({', '.join(parts)})"

    @begin_end()
    def check(self) -> bool:
        """Check if the current ILM step is complete.

        Verifies if the step and action are complete or if the index exists.

        Returns:
            bool: True if step is complete, False otherwise.

        Raises:
            :py:class:`es_wait.exceptions.IlmWaitError`: If index is not found.

        Example:
            >>> ilm = IlmStep(client, name="my-index")
            >>> ilm.check()  # Returns True if step is complete
            False
        """
        self.too_many_exceptions()
        if not self.client.indices.exists(index=self.name):
            self.exceptions_raised += 1
            self.add_exception(IlmWaitError(f'Index {self.name} not found.'))
            debug.lv1(f'Index {self.name} not found.')
            retval = False
        else:
            retval = self.phase_complete
        debug.lv5(f'Return value = {retval}')
        return retval
