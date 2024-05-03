"""Entity Exists Waiter"""

import typing as t
import logging
from ._base import Waiter

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger(__name__)

# pylint: disable=R0913


class Exists(Waiter):
    """Wait for an entity to 'exist' according to Elasticsearch"""

    IDX_OR_DS = ['index', 'data_stream', 'datastream', 'idx', 'ds']
    TEMPLATE = ['index_template', 'template', 'tmpl']
    COMPONENT = ['component_template', 'component', 'comp']

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = 1.5,
        timeout: float = 15,
        name: str = '',
        kind: t.Literal[
            'index',
            'data_stream',
            'index_template',
            'template',
            'component_template',
            'component',
        ] = '',
    ) -> None:

        super().__init__(client=client, pause=pause, timeout=timeout)
        #: The entity name
        self.name = name
        #: What kind of entity
        self.kind = kind
        self.empty_check('name')
        self.empty_check('kind')
        self.waitstr = f'for {self.kindmap} "{name}" to exist'
        logger.debug('Waiting %s...', self.waitstr)

    @property
    def check(self) -> bool:
        """
        Check if the named entity exists, based on :py:attr:`kind` and :py:attr:`name`.

        For indices and data_streams, the call is :py:meth:`indices.exists()
        <elasticsearch.client.IndicesClient.exists>`.

        For index templates, the call is :py:meth:`indices.exists_index_template()
        <elasticsearch.client.IndicesClient.exists_index_template>`.

        For component templates, it is :py:meth:`cluster.exists_component_template()
        <elasticsearch.client.ClusterClient.exists_component_template>`.

        The return value is the result of whichever call is made.

        :getter: Returns if the check was complete
        :type: bool
        """
        doit = {
            'index or data_stream': {
                'func': self.client.indices.exists,
                'kwargs': {'index': self.name},
            },
            'index template': {
                'func': self.client.indices.exists_index_template,
                'kwargs': {'name': self.name},
            },
            'component template': {
                'func': self.client.cluster.exists_component_template,
                'kwargs': {'name': self.name},
            },
            'FAIL': {'func': False, 'kwargs': {}},
        }
        return bool(doit[self.kindmap]['func'](**doit[self.kindmap]['kwargs']))

    @property
    def kindmap(
        self,
    ) -> t.Literal[
        'index or datastream', 'index template', 'component template', 'FAIL'
    ]:
        """
        This method helps map :py:attr:`kind` to the proper 'exists' API call, as well
        as accurately log what we're checking in :py:meth:`wait` logging.
        """
        if self.kind in self.IDX_OR_DS:
            return 'index or data_stream'
        if self.kind in self.TEMPLATE:
            return 'index template'
        if self.kind in self.COMPONENT:
            return 'component template'
        logger.error('%s is not an acceptable value for kind', self.kind)
        return 'FAIL'  # We should not see this, like, ever
