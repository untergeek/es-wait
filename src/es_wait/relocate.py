"""Index Relocation Waiter."""

# pylint: disable=R0902,R0913,R0917,W0718
import typing as t
import logging
from elasticsearch8.exceptions import TransportError
from ._base import Waiter
from .debug import debug, begin_end
from .defaults import RELOCATE

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger(__name__)


class Relocate(Waiter):
    """Wait for an index to finish relocating.

    Polls the cluster state to check if all shards for the index are in the
    STARTED state.

    Args:
        client (:py:class:`elasticsearch8.Elasticsearch`): Elasticsearch client.
        pause (float): Seconds between checks (default: 9.0).
        timeout (float): Max wait time in seconds (default: 3600.0).
        max_exceptions (int): Max allowed exceptions (default: 10).
        name (str, optional): Index name (default: None).

    Attributes:
        name (str): Index name.
        waitstr (str): Description of the wait operation.

    Raises:
        ValueError: If `name` is None.

    Example:
        >>> from elasticsearch8 import Elasticsearch
        >>> client = Elasticsearch()
        >>> relocator = Relocate(client, name="my-index")
        >>> relocator.wait()
    """

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = RELOCATE.get('pause', 9.0),
        timeout: float = RELOCATE.get('timeout', 3600.0),
        max_exceptions: int = RELOCATE.get('max_exceptions', 10),
        name: t.Optional[str] = None,
    ) -> None:
        """Initialize the Relocate waiter.

        Args:
            client (:py:class:`elasticsearch8.Elasticsearch`): Elasticsearch client.
            pause (float): Seconds between checks (default: 9.0).
            timeout (float): Max wait time in seconds (default: 3600.0).
            max_exceptions (int): Max allowed exceptions (default: 10).
            name (str, optional): Index name (default: None).

        Raises:
            ValueError: If `name` is None.

        Example:
            >>> from elasticsearch8 import Elasticsearch
            >>> client = Elasticsearch()
            >>> relocator = Relocate(client, name="my-index")
            >>> relocator.name
            'my-index'
        """
        super().__init__(
            client=client, pause=pause, timeout=timeout, max_exceptions=max_exceptions
        )
        debug.lv2('Initializing Relocate object...')
        self.name = name
        self._ensure_not_none('name')
        self.waitstr = f'for index "{self.name}" to finish relocating'
        self.announce()
        debug.lv3('Relocate object initialized')

    def __repr__(self) -> str:
        """Return a string representation of the Relocate instance.

        Returns:
            str: String representation including name, waitstr, and pause.

        Example:
            >>> relocator = Relocate(client, name="my-index", pause=5.0)
            >>> repr(relocator)
            "Relocate(name='my-index', waitstr='for index \"my-index\" to finish
            relocating', pause=5.0)"
        """
        parts = [
            f"name={self.name!r}",
            f"waitstr={self.waitstr!r}",
            f"pause={self.pause}",
        ]
        return f"{self.__class__.__name__}({', '.join(parts)})"

    @property
    @begin_end()
    def finished_state(self) -> bool:
        """Check if all shards in the index are STARTED.

        Returns:
            bool: True if all shards are STARTED, False otherwise.

        Example:
            >>> relocator = Relocate(client, name="my-index")
            >>> relocator.finished_state  # Checks shard states
            False
        """
        _ = self.routing_table()
        if not _:
            logger.warning(f'No routing table data for index "{self.name}"')
            debug.lv5('Return value = False')
            return False
        try:
            debug.lv4('TRY: Getting shard state data')
            retval = all(
                all(shard['state'] == "STARTED" for shard in shards)
                for shards in _.values()
            )
            debug.lv5(f'Return value = {retval}')
            return retval
        except KeyError as err:
            self.exceptions_raised += 1
            self.add_exception(err)
            logger.error(f'KeyError in finished_state for index "{self.name}"')
            debug.lv5('Return value = False')
            return False

    @begin_end()
    def routing_table(self) -> t.Dict[str, t.List[t.Dict[str, str]]]:
        """Get the shard routing table for the index.

        Calls the cluster state API with a filter to reduce response size.

        Returns:
            Dict[str, List[Dict[str, str]]]: Shard routing table.

        Raises:
            ValueError: If the API call fails.

        Example:
            >>> relocator = Relocate(client, name="my-index")
            >>> routing = relocator.routing_table()
            >>> isinstance(routing, dict)
            True
        """
        msg = f'Unable to get routing table data from cluster state for {self.name}'
        fpath = f'routing_table.indices.{self.name}'
        try:
            debug.lv4('TRY: Getting cluster state response')
            result = self.client.cluster.state(index=self.name, filter_path=fpath)
            debug.lv5(f'cluster.state response: {result}')
        except TransportError as exc:
            self.exceptions_raised += 1
            self.add_exception(exc)
            logger.critical(f'{msg} because of {exc}')
            debug.lv5('Return value = {}')
            return {}
        try:
            debug.lv4('TRY: Getting shard routing table data')
            retval = result['routing_table']['indices'][self.name]['shards']
            debug.lv5(f'Return value = {retval}')
            return retval
        except KeyError as err:
            self.exceptions_raised += 1
            self.add_exception(err)
            logger.error(f'{msg} because of {err}')
            debug.lv5('Return value = {}')
            return {}

    @begin_end()
    def check(self) -> bool:
        """Check if the index relocation is complete.

        Returns:
            bool: True if all shards are STARTED, False otherwise.

        Raises:
            :py:class:`elasticsearch8.exceptions.TransportError`: If API call fails.
            KeyError: If routing table data is missing.

        Example:
            >>> relocator = Relocate(client, name="my-index")
            >>> relocator.check()  # Returns True if relocation complete
            False
        """
        self.too_many_exceptions()
        try:
            debug.lv4('TRY: Getting finished_state value')
            finished = self.finished_state
        except (TransportError, KeyError) as err:
            self.exceptions_raised += 1
            self.add_exception(err)
            logger.error(f'Error checking for index "{self.name}": {err}')
            finished = False
        if finished:
            debug.lv1(f'Relocate Check for index: "{self.name}" has passed.')
        debug.lv5(f'Return value = {finished}')
        return finished
