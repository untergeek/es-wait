"""Helper and Utility Functions"""

import typing as t


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
