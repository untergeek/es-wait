"""Helper and utility functions for es-wait."""

import typing as t
import logging
from sys import version_info
from pprint import pformat
from .debug import debug, begin_end
from .defaults import HealthCheckDict

if t.TYPE_CHECKING:
    from elastic_transport import ObjectApiResponse

logger = logging.getLogger('es_wait.utils')


@begin_end()
def diagnosis_generator(ind: str, data: t.Sequence) -> t.Generator:
    """Yield diagnosis strings from health report data.

    Generates formatted strings for each diagnosis entry in the provided data.

    Args:
        ind (str): Name of the indicator.
        data (Sequence): List of diagnosis data from health report.

    Returns:
        Generator: Yields formatted diagnosis strings.

    Example:
        >>> data = [{"cause": "issue", "action": "fix", "affected_resources": []}]
        >>> list(diagnosis_generator("test", data))
        ['INDICATOR: test: DIAGNOSIS #0: CAUSE: issue',
         'INDICATOR: test: DIAGNOSIS #0: ACTION: fix',
         'INDICATOR: test: DIAGNOSIS #0: AFFECTED_RESOURCES: []']
    """
    diag_keys = ['cause', 'action', 'affected_resources']
    for idx, diag in enumerate(data):
        for key in diag_keys:
            yield f'INDICATOR: {ind}: DIAGNOSIS #{idx}: {key.upper()}: {diag[key]}'


@begin_end()
def impact_generator(ind: str, data: t.Sequence) -> t.Generator:
    """Yield impact strings from health report data.

    Generates formatted strings for each impact entry in the provided data.

    Args:
        ind (str): Name of the indicator.
        data (Sequence): List of impact data from health report.

    Returns:
        Generator: Yields formatted impact strings.

    Example:
        >>> data = [{"severity": "high", "description": "issue", "impact_areas": []}]
        >>> list(impact_generator("test", data))
        ['INDICATOR: test: IMPACT AREA #0: SEVERITY: high',
         'INDICATOR: test: IMPACT AREA #0: DESCRIPTION: issue',
         'INDICATOR: test: IMPACT AREA #0: IMPACT_AREAS: []']
    """
    impact_keys = ['severity', 'description', 'impact_areas']
    for idx, impact in enumerate(data):
        for key in impact_keys:
            yield f'INDICATOR: {ind}: IMPACT AREA #{idx}: {key.upper()}: {impact[key]}'


@begin_end()
def indicator_generator(ind: str, data: t.Dict) -> t.Generator:
    """Yield symptom, details, impacts, and diagnosis strings.

    Generates formatted strings for indicator data, including symptoms, details,
    and calls to diagnosis and impact generators.

    Args:
        ind (str): Name of the indicator.
        data (Dict): Indicator data from health report.

    Returns:
        Generator: Yields formatted indicator strings.

    Example:
        >>> data = {"symptom": "issue", "details": "details", "impacts": [],
        ...         "diagnosis": []}
        >>> list(indicator_generator("test", data))
        ['INDICATOR: test: SYMPTOM: issue',
         'INDICATOR: test: DETAILS: details']
    """
    ind_keys = ['symptom', 'details', 'impacts', 'diagnosis']
    gen_map = {'diagnosis': diagnosis_generator, 'impacts': impact_generator}
    for key in ind_keys:
        if key in gen_map:
            func = gen_map[key]
            yield from func(ind, data[key])
        else:
            yield f'INDICATOR: {ind}: {key.upper()}: {data[key]}'


@begin_end()
def healthchk_result(data: "ObjectApiResponse", check_for: HealthCheckDict) -> bool:
    """Check Elasticsearch health check data.

    Validates if the health check response matches expected key-value pairs.

    Args:
        data (:py:class:`elastic_transport.ObjectApiResponse`): Health check data.
        check_for (:py:class:`es_wait.defaults.HealthCheckDict`): Expected
            key-value pairs.

    Returns:
        bool: True if all conditions match, False otherwise.

    Raises:
        KeyError: If a required key is missing in the response.

    Example:
        >>> from elastic_transport import ObjectApiResponse
        >>> data = ObjectApiResponse({"status": "green"})
        >>> check_for = {"status": "green"}
        >>> healthchk_result(data, check_for)
        True
    """
    output = dict(data)
    check = True
    for key, value in check_for.items():
        if key not in output:
            raise KeyError(f'Key "{key}" not in cluster health output')
        if output[key] != value:
            msg = (
                f'NO MATCH: Value for key "{value}", health check output: '
                f'{output[key]}'
            )
            check = False
        else:
            msg = (
                f'MATCH: Value for key "{value}", health check output: '
                f'{output[key]}'
            )
        debug.lv3(msg)
    debug.lv5(f'Return value = {check}')
    return check


@begin_end()
def health_report(data: "ObjectApiResponse") -> None:
    """Log health report data from Elasticsearch.

    Logs details for non-green health statuses, including indicators.

    Args:
        data (:py:class:`elastic_transport.ObjectApiResponse`): Health report data.

    Example:
        >>> from elastic_transport import ObjectApiResponse
        >>> data = ObjectApiResponse({"status": "red", "indicators": {}})
        >>> health_report(data)  # Logs: HEALTH REPORT: STATUS: RED
    """
    rpt = dict(data)
    try:
        debug.lv4('TRY: Looping over health report data')
        if rpt['status'] != 'green':
            logger.info(f"HEALTH REPORT: STATUS: {rpt['status'].upper()}")
            loop_health_indicators(rpt['indicators'])
    except KeyError as err:
        logger.error(f'Health report data: {rpt}, error: {prettystr(err)}')


@begin_end()
def loop_health_indicators(inds: t.Dict) -> None:
    """Log health report indicators.

    Iterates through indicators and logs details for non-green statuses.

    Args:
        inds (Dict): Health report indicators.

    Example:
        >>> inds = {"test": {"status": "red", "symptom": "issue"}}
        >>> loop_health_indicators(inds)
        ... # Logs: HEALTH REPORT: INDICATOR: test: SYMPTOM: issue
    """
    for ind in inds:
        if isinstance(ind, str):
            if inds[ind]['status'] != 'green':
                for line in indicator_generator(ind, inds[ind]):
                    logger.info(f'HEALTH REPORT: {line}')


@begin_end(begin=5, end=5)
def prettystr(*args, **kwargs) -> str:
    """Format objects as readable strings.

    Wraps :py:func:`pprint.pformat` with custom defaults for indent and sorting.

    Args:
        *args: Objects to format.
        **kwargs: Formatting options (e.g., indent, width).

    Returns:
        str: Formatted string representation.

    Example:
        >>> data = {"key": "value"}
        >>> print(prettystr(data))
        {
          'key': 'value'
        }
    """
    defaults = [
        ('indent', 2),
        ('width', 80),
        ('depth', None),
        ('compact', False),
        ('sort_dicts', False),
    ]
    if version_info[0] >= 3 and version_info[1] >= 10:
        defaults.append(('underscore_numbers', False))
    kw = {}
    for tup in defaults:
        key, default = tup
        kw[key] = kwargs[key] if key in kwargs else default
    retval = f"\n{pformat(*args, **kw)}"
    debug.lv5(f'Return value = {retval}')
    return retval
