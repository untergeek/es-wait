"""Entity Exists Waiter."""

# pylint: disable=R0902,R0913,R0917,W0718
import typing as t
import logging
from elasticsearch8.exceptions import TransportError
from ._base import Waiter
from .debug import debug, begin_end
from .defaults import EXISTS, ExistsTypes

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger(__name__)


class Exists(Waiter):
    """Wait for an Elasticsearch entity to exist.

    Polls the cluster to check if an entity (e.g., index, data stream) exists.

    Args:
        client (:py:class:`elasticsearch8.Elasticsearch`): Elasticsearch client.
        pause (float): Seconds between checks (default: 1.5).
        timeout (float): Max wait time in seconds (default: 10.0).
        max_exceptions (int): Max allowed exceptions (default: 10).
        name (str): Entity name (default: '').
        kind (:py:class:`es_wait.defaults.ExistsTypes`): Entity type
            (default: 'index').

    Attributes:
        name (str): Entity name.
        kind (:py:class:`es_wait.defaults.ExistsTypes`): Entity type.
        waitstr (str): Description of the wait operation.

    Raises:
        ValueError: If `name` is empty or `kind` is invalid.

    Example:
        >>> from elasticsearch8 import Elasticsearch
        >>> client = Elasticsearch()
        >>> exists = Exists(client, name="my-index", kind="index")
        >>> exists.wait()
    """

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = EXISTS.get('pause', 1.5),
        timeout: float = EXISTS.get('timeout', 10.0),
        max_exceptions: int = EXISTS.get('max_exceptions', 10),
        name: str = '',
        kind: ExistsTypes = 'index',
    ) -> None:
        """Initialize the Exists waiter.

        Args:
            client (:py:class:`elasticsearch8.Elasticsearch`): Elasticsearch client.
            pause (float): Seconds between checks (default: 1.5).
            timeout (float): Max wait time in seconds (default: 10.0).
            max_exceptions (int): Max allowed exceptions (default: 10).
            name (str): Entity name (default: '').
            kind (:py:class:`es_wait.defaults.ExistsTypes`): Entity type
                (default: 'index').

        Raises:
            ValueError: If `name` is empty or `kind` is invalid.

        Example:
            >>> from elasticsearch8 import Elasticsearch
            >>> client = Elasticsearch()
            >>> exists = Exists(client, name="my-index", kind="index")
            >>> exists.kind
            'index'
        """
        super().__init__(
            client=client, pause=pause, timeout=timeout, max_exceptions=max_exceptions
        )
        debug.lv2('Initializing Exists object...')
        self.name = name
        if kind not in EXISTS['types']:
            msg = f'kind must be one of {", ".join(EXISTS["types"])}'
            logger.error(msg)
            raise ValueError(msg)
        self.kind = kind
        self._ensure_not_none('name')
        self.waitstr = f'for {kind} "{name}" to exist'
        self.announce()
        debug.lv3('Exists object initialized')

    def __repr__(self) -> str:
        """Return a string representation of the Exists instance.

        Returns:
            str: String representation including name, kind, waitstr, and pause.

        Example:
            >>> exists = Exists(client, name="my-index", kind="index", pause=1.5)
            >>> repr(exists)
            'Exists(name="my-index", kind="index", waitstr="for index \"my-index\"
            to exist", pause=1.5)'
        """
        parts = [
            f"name={self.name!r}",
            f"kind={self.kind!r}",
            f"waitstr={self.waitstr!r}",
            f"pause={self.pause}",
        ]
        return f"{self.__class__.__name__}({', '.join(parts)})"

    @begin_end()
    def check(self) -> bool:
        """Check if the named entity exists.

        Uses the appropriate API call based on the entity kind.

        Returns:
            bool: True if the entity exists, False otherwise.

        Raises:
            :py:class:`elasticsearch8.exceptions.TransportError`: If API call fails.

        Example:
            >>> exists = Exists(client, name="my-index", kind="index")
            >>> exists.check()  # Returns True if index exists
            False
        """
        func, kwargs = self.func_map()
        self.too_many_exceptions()
        try:
            debug.lv4('TRY: Getting boolean value from function call')
            retval = bool(func(**kwargs))
        except TransportError as err:
            self.exceptions_raised += 1
            self.add_exception(err)
            msg = f'Error checking for {self.kind} "{self.name}": {err}'
            logger.error(msg)
            retval = False
        debug.lv5(f'Return value = {retval}')
        return retval

    @begin_end()
    def func_map(self) -> t.Tuple[t.Callable, t.Dict]:
        """Map entity kind to the appropriate API call.

        Returns a tuple of the API function and its keyword arguments.

        Returns:
            Tuple[Callable, Dict]: Function to call and its keyword arguments.

        Example:
            >>> exists = Exists(client, name="my-index", kind="index")
            >>> func, kwargs = exists.func_map()
            >>> callable(func) and isinstance(kwargs, dict)
            True
        """
        _ = {
            'index': (self.client.indices.exists, {'index': self.name}),
            'data_stream': (self.client.indices.exists, {'index': self.name}),
            'index_template': (
                self.client.indices.exists_index_template,
                {'name': self.name},
            ),
            'component_template': (
                self.client.cluster.exists_component_template,
                {'name': self.name},
            ),
        }
        retval = _[self.kind]
        debug.lv5(f'Return value = {retval}')
        return retval
