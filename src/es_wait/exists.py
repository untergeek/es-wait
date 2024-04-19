"""Entity Exists"""
import typing as t
import logging
from elasticsearch8 import Elasticsearch
from .base import Waiter

# pylint: disable=missing-docstring,too-many-arguments

class Exists(Waiter):
    """Class Definition"""
    ACTIONS: t.Optional[str] = None
    IDX_OR_DS = ['index', 'datastream', 'idx', 'ds']
    TEMPLATE = ['index_template', 'template', 'tmpl']
    COMPONENT = ['component_template', 'component', 'comp']
    def __init__(
            self,
            client: Elasticsearch,
            action: t.Optional[str] = None,
            pause: float = 1.5,
            timeout: float = 15,
            name: str = None,
            kind: str = None
        ) -> None:
        super().__init__(client=client, action=action, pause=pause, timeout=timeout)
        self.logger = logging.getLogger('es_wait.Exists')
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
            'index or datastream': self.client.indices.exists(index=self.name),
            'index template': self.client.indices.exists_index_template(name=self.name),
            'component template': self.client.cluster.exists_component_template(name=self.name),
            'FAIL': False
        }
        return doit[self.kindmap]

    @property
    def kindmap(self) -> t.Literal[
            'index or datastream', 'index template', 'component template', 'FAIL']:
        """This is a little way to ensure many possibilities come down to one"""
        if self.kind in self.IDX_OR_DS:
            return 'index or datastream'
        if self.kind in self.TEMPLATE:
            return 'index template'
        if self.kind in self.COMPONENT:
            return 'component template'
        self.logger.error('%s is not an acceptable value for kind', self.kind)
        return 'FAIL' # We should not see this, like, ever
