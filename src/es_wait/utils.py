"""Helper and Utility Functions"""

import typing as t
import logging
from sys import version_info
from pprint import pformat
import tiered_debug as debug
from .defaults import HealthCheckDict

if t.TYPE_CHECKING:
    from elastic_transport import ObjectApiResponse

logger = logging.getLogger('es_wait.utils')


def diagnosis_generator(ind: str, data: t.Sequence) -> t.Generator:
    """
    Yield diagnosis strings from the provided data
    :param data: The list from health_report['indicators'][ind]['diagnosis']
    :type data: list
    """
    debug.lv2('Starting function...')
    diag_keys = ['cause', 'action', 'affected_resources']
    for idx, diag in enumerate(data):
        for key in diag_keys:
            yield f'INDICATOR: {ind}: DIAGNOSIS #{idx}: {key.upper()}: {diag[key]}'
    debug.lv3('Exiting function')


def impact_generator(ind: str, data: t.Sequence) -> t.Generator:
    """
    Yield impact strings from the provided data
    :param data: The list from health_report['indicators'][ind]['impact']
    :type data: list
    """
    debug.lv2('Starting function...')
    impact_keys = ['severity', 'description', 'impact_areas']
    for idx, impact in enumerate(data):
        for key in impact_keys:
            yield f'INDICATOR: {ind}: IMPACT AREA #{idx}: {key.upper()}: {impact[key]}'
    debug.lv3('Exiting function')


def indicator_generator(ind: str, data: t.Dict) -> t.Generator:
    """
    Yield symptom, details, and impacts and diagnosis strings for any indicators
    :param data: Data from health_report['indicators'][ind]
    :type data: dict
    """
    debug.lv2('Starting function...')
    ind_keys = ['symptom', 'details', 'impacts', 'diagnosis']
    gen_map = {'diagnosis': diagnosis_generator, 'impacts': impact_generator}
    for key in ind_keys:
        if key in gen_map:
            func = gen_map[key]
            yield from func(ind, data[key])
        else:
            yield f'INDICATOR: {ind}: {key.upper()}: {data[key]}'
    debug.lv3('Exiting function')


def healthchk_result(data: "ObjectApiResponse", check_for: HealthCheckDict) -> bool:
    """
    Check the health check data from
    :py:meth:`client.health_check() <elasticsearch.client.health_check>`.

    If multiple keys are provided in `check_for`, all must key/value pairs must
    match for a ``True`` response.

    If the expected response(s) are in the data, return True, otherwise False.
    Debug log the results.

    :param data: The health check data
    :type data: :py:obj:`ObjectApiResponse <elastic_transport.ObjectApiResponse>`

    :param check_for: The expected response
    :type check_for: HealthCheckDict

    :returns: True if the expected response is in the data, otherwise False
    :rtype: bool
    """
    debug.lv2('Starting function...')
    output = dict(data)
    check = True
    for key, value in check_for.items():
        # First, verify that the key is in output
        if key not in output:
            raise KeyError(f'Key "{key}" not in cluster health output')
        # Verify that the output matches the expected value
        if output[key] != value:
            msg = (
                f'NO MATCH: Value for key "{value}", health check output: '
                f'{output[key]}'
            )
            check = False  # We do not match
        else:
            msg = (
                f'MATCH: Value for key "{value}", health check output: '
                f'{output[key]}'
            )
        debug.lv3(msg)
    debug.lv3('Exiting function, returning value')
    debug.lv5(f'Value = {check}')
    return check


def health_report(data: "ObjectApiResponse") -> None:
    """
    Log the health report data from
    :py:meth:`client.health_report() <elasticsearch.client.health_report>`.

    :param data: The health report data
    :type data: :py:obj:`ObjectApiResponse <elastic_transport.ObjectApiResponse>`
    """
    debug.lv2('Starting function...')
    rpt = dict(data)

    try:
        debug.lv4('TRY: Looping over health report data')
        if rpt['status'] != 'green':
            logger.info(f"HEALTH REPORT: STATUS: {rpt['status'].upper()}")
            loop_health_indicators(rpt['indicators'])
    except KeyError as err:
        logger.error(f'Health report data: {rpt}, error: {prettystr(err)}')
    debug.lv3('Exiting function')


def loop_health_indicators(inds: t.Dict) -> None:
    """
    Loop through the indicators and log the data
    :param inds: The health report indicators
    :type inds: dict
    """
    debug.lv2('Starting function...')
    for ind in inds:
        if isinstance(ind, str):
            if inds[ind]['status'] != 'green':
                for line in indicator_generator(ind, inds[ind]):
                    logger.info(f'HEALTH REPORT: {line}')
    debug.lv3('Exiting function')


def prettystr(*args, **kwargs) -> str:
    """
    A (nearly) straight up wrapper for :py:meth:`pprint.pformat()
    <pprint.PrettyPrinter.pformat>`, except that we provide our own default values
    for `indent` (`2`) and `sort_dicts` (`False`). Primarily for debug logging and
    showing more readable dictionaries.

    'Return the formatted representation of object as a string. indent, width,
    depth, compact, sort_dicts and underscore_numbers are passed to the
    PrettyPrinter constructor as formatting parameters' (from pprint
    documentation).
    """
    debug.lv5('Starting function...')
    defaults = [
        ('indent', 2),
        ('width', 80),
        ('depth', None),
        ('compact', False),
        ('sort_dicts', False),
    ]
    if version_info[0] >= 3 and version_info[1] >= 10:
        # underscore_numbers only works in 3.10 and up
        defaults.append(('underscore_numbers', False))
    kw = {}
    for tup in defaults:
        key, default = tup
        kw[key] = kwargs[key] if key in kwargs else default
    # newline in front so it's always clean
    retval = f"\n{pformat(*args, **kw)}"  # type: ignore
    debug.lv5('Exiting function, returning value')
    debug.lv5(f'Value = {retval}')
    return retval
