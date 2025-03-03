"""Helper and Utility Functions"""

import typing as t
from sys import version_info
from pprint import pformat


def diagnosis_generator(ind: str, data: t.Sequence) -> t.Generator:
    """
    Yield diagnosis strings from the provided data
    :param data: The list from health_report['indicators'][ind]['diagnosis']
    :type data: list
    """
    diag_keys = ['cause', 'action', 'affected_resources']
    for idx, diag in enumerate(data):
        for key in diag_keys:
            yield f'INDICATOR: {ind}: DIAGNOSIS #{idx}: {key.upper()}: {diag[key]}'


def impact_generator(ind: str, data: t.Sequence) -> t.Generator:
    """
    Yield impact strings from the provided data
    :param data: The list from health_report['indicators'][ind]['impact']
    :type data: list
    """
    impact_keys = ['severity', 'description', 'impact_areas']
    for idx, impact in enumerate(data):
        for key in impact_keys:
            yield f'INDICATOR: {ind}: IMPACT AREA #{idx}: {key.upper()}: {impact[key]}'


def indicator_generator(ind: str, data: t.Dict) -> t.Generator:
    """
    Yield symptom, details, and impacts and diagnosis strings for any indicators
    :param data: Data from health_report['indicators'][ind]
    :type data: dict
    """
    ind_keys = ['symptom', 'details', 'impacts', 'diagnosis']
    gen_map = {'diagnosis': diagnosis_generator, 'impacts': impact_generator}
    for key in ind_keys:
        if key in gen_map:
            func = gen_map[key]
            for line in func(ind, data[key]):
                yield line
        else:
            yield f'INDICATOR: {ind}: {key.upper()}: {data[key]}'


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
    return f"\n{pformat(*args, **kw)}"  # type: ignore
