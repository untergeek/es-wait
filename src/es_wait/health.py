"""Health Check Waiter."""

# pylint: disable=R0902,R0913,R0917,W0718
import typing as t
import logging
from elasticsearch8.exceptions import TransportError
from ._base import Waiter
from .debug import debug, begin_end
from .defaults import HealthCheckDict, HealthTypes, HEALTH
from .utils import healthchk_result, prettystr

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger(__name__)

STATUS_CHK: HealthCheckDict = {'status': 'green'}
RELO_CHK: HealthCheckDict = {'relocating_shards': 0}
WAITSTR_MAP = {
    'status': 'for cluster health to show green status',
    'relocation': 'for cluster health to show zero relocating shards',
    'cluster_routing': (
        'for cluster health to show zero relocating shards across all indices'
    ),
}


class Health(Waiter):
    """Wait for Elasticsearch cluster health conditions.

    Polls the cluster health to check conditions like green status or no
    relocating shards.

    Args:
        client (:py:class:`elasticsearch8.Elasticsearch`): Elasticsearch client.
        pause (float): Seconds between checks (default: 1.5).
        timeout (float): Max wait time in seconds (default: 15.0).
        max_exceptions (int): Max allowed exceptions (default: 10).
        check_type (:py:class:`es_wait.defaults.HealthTypes`): Health check type
            (default: 'status').
        indices (Union[str, List[str], None]): Indices to check (default: None).
        check_for (:py:class:`es_wait.defaults.HealthCheckDict`, optional):
            Custom conditions (default: None).

    Attributes:
        check_type (:py:class:`es_wait.defaults.HealthTypes`): Health check type.
        indices (str): Comma-separated indices or '*' for all.
        check_for (:py:class:`es_wait.defaults.HealthCheckDict`): Expected
            conditions.
        waitstr (str): Description of the wait operation.
        do_health_report (bool): Always True to log health report on failure.

    Example:
        >>> from elasticsearch8 import Elasticsearch
        >>> client = Elasticsearch()
        >>> health = Health(client, check_type="status")
        >>> health.wait()
    """

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = HEALTH.get('pause', 1.5),
        timeout: float = HEALTH.get('timeout', 15.0),
        max_exceptions: int = HEALTH.get('max_exceptions', 10),
        check_type: HealthTypes = 'status',
        indices: t.Optional[t.Union[str, t.List[str]]] = None,
        check_for: t.Optional[HealthCheckDict] = None,
    ) -> None:
        """Initialize the Health waiter.

        Args:
            client (:py:class:`elasticsearch8.Elasticsearch`): Elasticsearch client.
            pause (float): Seconds between checks (default: 1.5).
            timeout (float): Max wait time in seconds (default: 15.0).
            max_exceptions (int): Max allowed exceptions (default: 10).
            check_type (:py:class:`es_wait.defaults.HealthTypes`): Health check
                type (default: 'status').
            indices (Union[str, List[str], None]): Indices to check (default: None).
            check_for (:py:class:`es_wait.defaults.HealthCheckDict`, optional):
                Custom conditions (default: None).

        Example:
            >>> from elasticsearch8 import Elasticsearch
            >>> client = Elasticsearch()
            >>> health = Health(client, check_type="status")
            >>> health.check_type
            'status'
        """
        super().__init__(
            client=client, pause=pause, timeout=timeout, max_exceptions=max_exceptions
        )
        debug.lv2('Initializing Health object...')
        self.check_type = check_type
        if self.check_type == 'cluster_routing' and indices is not None:
            logger.warning(
                "For 'cluster_routing', 'indices' is ignored. Checking all indices."
            )
        self.do_health_report = True
        self.indices = (
            ','.join(indices) if isinstance(indices, list) else indices or '*'
        )
        self.check_for = check_for or HEALTH['types'][check_type]
        self.waitstr = WAITSTR_MAP[check_type]
        self.announce()
        debug.lv3('Health object initialized')

    def __repr__(self) -> str:
        """Return a string representation of the Health instance.

        Returns:
            str: String representation including check_type, indices, waitstr,
                and pause.

        Example:
            >>> health = Health(client, check_type="status", pause=1.5)
            >>> repr(health)
            'Health(check_type="status", indices="*", waitstr="for cluster health
            to show green status", pause=1.5)'
        """
        parts = [
            f"check_type={self.check_type!r}",
            f"indices={self.indices!r}",
            f"waitstr={self.waitstr!r}",
            f"pause={self.pause}",
        ]
        return f"{self.__class__.__name__}({', '.join(parts)})"

    @property
    def filter_path(self) -> str:
        """Get the filter path for health check API calls.

        Returns:
            str: Comma-separated keys from check_for for filtering.

        Example:
            >>> health = Health(client, check_for={"status": "green"})
            >>> health.filter_path
            'status'
        """
        return ','.join(self.check_for.keys())

    @begin_end()
    def check(self) -> bool:
        """Check if the health condition is met.

        Calls the cluster.health API and validates the response against check_for.

        Returns:
            bool: True if conditions are met, False otherwise.

        Raises:
            :py:class:`elasticsearch8.exceptions.TransportError`: If API call fails.

        Example:
            >>> health = Health(client, check_type="status")
            >>> health.check()  # Returns True if cluster is green
            False
        """
        self.too_many_exceptions()
        target = self.indices if self.check_type != 'cluster_routing' else '*'
        if self.check_type == 'cluster_routing':
            logger.info('Doing cluster_routing health check. Checking all indices.')
        try:
            debug.lv4('TRY: Getting health check response')
            response = self.client.cluster.health(
                index=target, filter_path=self.filter_path
            )
            debug.lv5(f'cluster.health response: {response}')
            result = healthchk_result(response, self.check_for)
            if result:
                retval = result
            else:
                retval = False
        except TransportError as err:
            self.exceptions_raised += 1
            self.add_exception(err)
            logger.error(f'Error checking health: {prettystr(err)}')
            retval = False
        debug.lv5(f'Return value = {retval}')
        return retval
