"""Entity Exists Waiter"""

# pylint: disable=R0902,R0913,R0917,W0718
import typing as t
import logging
import tiered_debug as debug
from elasticsearch8.exceptions import TransportError
from ._base import Waiter
from .defaults import EXISTS, ExistsTypes

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger(__name__)


class Exists(Waiter):
    """Wait for an entity to 'exist' according to Elasticsearch"""

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = EXISTS.get('pause', 1.5),
        timeout: float = EXISTS.get('timeout', 10.0),
        max_exceptions: int = EXISTS.get('max_exceptions', 10),
        name: str = '',
        kind: ExistsTypes = 'index',
    ) -> None:
        """Init the Exists class

        :param client: The Elasticsearch client
        :type client: Elasticsearch
        :param pause: The delay between checks. Default is 1.5
        :type pause: float
        :param timeout: How long is too long. Default is 10.0
        :type timeout: float
        :param max_exceptions: The maximum number of exceptions to allow. Default is 10
        :type max_exceptions: int
        :param name: The entity name
        :type name: str
        :param kind: What kind of entity
        :type kind: ExistsTypes

        :raises ValueError: If kind is not one of the valid types
        """
        super().__init__(
            client=client, pause=pause, timeout=timeout, max_exceptions=max_exceptions
        )
        debug.lv2('Initializing Exists object...')
        #: The entity name
        self.name = name
        if kind not in EXISTS['types']:
            msg = f'kind must be one of {", ".join(EXISTS["types"])}'
            logger.error(msg)
            raise ValueError(msg)
        #: What kind of entity
        self.kind = kind
        self._ensure_not_none('name')
        self.waitstr = f'for {kind} "{name}" to exist'
        self.announce()
        debug.lv3('Exists object initialized')

    def check(self) -> bool:
        """
        Check if the named entity exists in Elasticsearch. The proper function
        call is returned by :py:meth:`func_map() <es_wait.exists.func_map>`, which
        returns a tuple of the function to call and the keyword arguments to pass
        to that function.

            - 'index': Checks for an index using
                :py:meth:`indices.exists() <elasticsearch.client.IndicesClient.exists>`
            - 'data_stream': Checks for a data_stream using
                :py:meth:`indices.exists() <elasticsearch.client.IndicesClient.exists>`
            - 'index_template': Checks for an index template using
                :py:meth:`indices.exists_index_template()
                <elasticsearch.client.IndicesClient.exists_index_template>`
            - 'component_template': Checks for a component template using
                :py:meth:`cluster.exists_component_template()
                <elasticsearch.client.ClusterClient.exists_component_template>`

        Returns:
            bool: True if the entity exists, False otherwise or if an error occurs.
        """
        debug.lv2('Starting method...')
        func, kwargs = self.func_map()
        self.too_many_exceptions()
        try:
            debug.lv4('TRY: Getting boolean value from function call')
            retval = bool(func(**kwargs))
        except TransportError as err:
            self.exceptions_raised += 1
            self.add_exception(err)  # Append the error to self._exceptions
            msg = f'Error checking for {self.kind} "{self.name}": {err}'
            logger.error(msg)
            retval = False
        debug.lv3('Exiting method, returning value')
        debug.lv5(f'Value = {retval}')
        return retval

    def func_map(self) -> t.Tuple[t.Callable, t.Dict]:
        """
        This method maps :py:attr:`kind` to the proper function to call based
        on the kind of entity we're checking and the keyword arguments to pass
        to that function.

        For indices and data_streams, the call is :py:meth:`indices.exists()
        <elasticsearch.client.IndicesClient.exists>`. The keyword argument is
        ``index``, with the value :py:attr:`name`.

        For index templates, the call is :py:meth:`indices.exists_index_template()
        <elasticsearch.client.IndicesClient.exists_index_template>`. The keyword
        argument is ``name``, with the value :py:attr:`name`.

        For component templates, it is :py:meth:`cluster.exists_component_template()
        <elasticsearch.client.ClusterClient.exists_component_template>`. The keyword
        argument is ``name``, with the value :py:attr:`name`.

        :returns: Tuple of the function to call and the keyword arguments
        :rtype: tuple
        """
        debug.lv2('Starting method...')
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
        retval = _[self.kind]  # __init__ ensures self.kind is valid
        debug.lv3('Exiting method, returning value')
        debug.lv5(f'Value = {retval}')
        return retval
