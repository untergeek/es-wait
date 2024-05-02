"""es_wait Exceptions"""


class EsWaitException(Exception):
    """Base Exception Class for es_wait"""


class IlmWaitError(EsWaitException):
    """Any ILM-related Exception"""
