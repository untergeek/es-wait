"""Entity Exists"""

import typing as t
import logging
from ._base import Waiter

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

# pylint: disable=missing-docstring,too-many-arguments


class Exists(Waiter):
    """Class Definition"""

    IDX_OR_DS = ['index', 'data_stream', 'datastream', 'idx', 'ds']
    TEMPLATE = ['index_template', 'template', 'tmpl']
    COMPONENT = ['component_template', 'component', 'comp']

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = 1.5,
        timeout: float = 15,
        name: str = '',
        kind: str = '',
    ) -> None:
        self.logger = logging.getLogger('es_wait.Exists')
        super().__init__(client=client, pause=pause, timeout=timeout)
        self.name = name
        self.kind = kind
        self.empty_check('name')
        self.empty_check('kind')
        self.checkid = f'check for {self.kindmap} {name} to exist'

    @property
    def check(self) -> bool:
        """
        Check if the named entity exists, based on kind
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
        """This is a little way to ensure many possibilities come down to one"""
        if self.kind in self.IDX_OR_DS:
            return 'index or data_stream'
        if self.kind in self.TEMPLATE:
            return 'index template'
        if self.kind in self.COMPONENT:
            return 'component template'
        self.logger.error('%s is not an acceptable value for kind', self.kind)
        return 'FAIL'  # We should not see this, like, ever
