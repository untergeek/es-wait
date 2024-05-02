"""ILM Phase and Step Checks"""

import typing as t
import logging
from dotmap import DotMap
from elasticsearch8.exceptions import NotFoundError
from ._base import Waiter
from .exceptions import IlmWaitError

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger('es_wait.IndexLifecycle')

# pylint: disable=R0913


class IndexLifecycle(Waiter):
    """ILM Step and Phase Parent Class"""

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = 1,
        timeout: float = -1,
        name: t.Union[str, None] = None,
    ) -> None:

        super().__init__(client=client, pause=pause, timeout=timeout)
        self.name = name
        self.empty_check('name')

    def get_explain_data(self) -> t.Union[t.Dict, None]:
        """
        Calls `client.indices.`
        :py:meth:`~.elasticsearch.client.IlmClient.explain_lifecycle` with an index
        name and returns the resulting response.
        """
        try:
            resp = self.client.ilm.explain_lifecycle(index=self.name)
            logger.debug('ILM Explain response: %s', self.prettystr(resp))
        except NotFoundError as exc:
            msg = (
                f'Datastream/Index Name changed. {self.name} was not found. '
                f'This is likely due to the index name suddenly changing, as with '
                f'searchable snapshot mounts.'
            )
            logger.error(msg)
            raise exc  # re-raise the original. Just wanted to log here.
        except Exception as err:
            msg = f'Unable to get ILM information for index {self.name}'
            logger.critical(msg)
            raise IlmWaitError(f'{msg}. Exception: {self.prettystr(err)}') from err
        retval = resp['indices'][self.name]
        return retval


class IlmPhase(IndexLifecycle):
    """
    ILM Phase class (child of class IndexLifecycle)

    It should be noted that the default ILM polling interval in Elasticsearch is 10
    minutes. Setting pause and timeout accordingly is a good idea.
    """

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = 1,
        timeout: float = -1,
        name: t.Union[str, None] = None,
        phase: t.Union[str, None] = None,
    ) -> None:
        self.logger = logging.getLogger('es_wait.IlmPhase')
        super().__init__(client=client, pause=pause, timeout=timeout, name=name)
        self.phase = phase
        self.empty_check('phase')
        self.checkid = (
            f'check for completion of ILM transition for {self.name} to '
            f'phase "{self.phase}"'
        )

    @property
    def check(self) -> bool:
        """
        Check for ILM phase change completion.  It will return ``True`` if the expected
        phase and the actually collected phase match.

        Upstream callers need to try/catch any of KeyError (index name changed),
        :py:exc:`~.elasticsearch.exceptions.NotFoundError`, and
        :py:exc:`~.es_wait.exceptions.IlmWaitError`.

        We cannot not be responsible for retrying with a changed name as it's not in
        our scope as a "waiter"
        """
        explain = DotMap(self.get_explain_data())
        return bool(explain.phase == self.phase)


class IlmStep(IndexLifecycle):
    """
    ILM Step class (child of class IndexLifecycle)

    It should be noted that the default ILM polling interval in Elasticsearch is 10
    minutes. Setting pause and timeout accordingly is a good idea.
    """

    def __init__(
        self,
        client: 'Elasticsearch',
        pause: float = 1,
        timeout: float = -1,
        name: t.Union[str, None] = None,
    ) -> None:
        self.logger = logging.getLogger('es_wait.IlmStep')
        super().__init__(client=client, pause=pause, timeout=timeout, name=name)
        self.checkid = f'check for completion of ILM step for {self.name}'

    @property
    def check(self) -> bool:
        """
        Check for ILM step completion.  It will return ``True`` if the step and
        action values are both ``complete``

        Upstream callers need to try/catch any of KeyError (index name changed),
        :py:exc:`~.elasticsearch.exceptions.NotFoundError`, and
        :py:exc:`~.es_wait.exceptions.IlmWaitError`.

        We cannot not be responsible for retrying with a changed name as it's not in
        our scope as a "waiter"
        """
        explain = DotMap(action='no', step='no')  # Set defaults so the return works
        try:
            explain = DotMap(self.get_explain_data())
        except NotFoundError as err:
            if self.client.indices.exists(index=self.name):
                msg = (
                    f'NotFoundError encountered. However, index {self.name} has been '
                    f'confirmed to exist, so we continue to retry...'
                )
                self.logger.debug(msg)
            else:
                raise err
        return bool(explain.action == 'complete' and explain.step == 'complete')
