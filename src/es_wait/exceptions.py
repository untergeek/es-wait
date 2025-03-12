"""es_wait Exceptions"""

from typing import Any, Tuple


class EsWaitException(Exception):
    """Base Exception Class for es_wait package

    For the 'errors' attribute, errors are ordered from
    most recently raised (index=0) to least recently raised (index=N)
    """

    def __init__(self, message: Any, errors: Tuple[Exception, ...] = ()):
        super().__init__(message)
        self.message = message
        self.errors = tuple(errors)

    def __repr__(self) -> str:
        parts = [repr(self.message)]
        if self.errors:
            parts.append(f"errors={self.errors!r}")
        return f'{self.__class__.__name__}({", ".join(parts)})'

    def __str__(self) -> str:
        return str(self.message)


class EsWaitFatal(EsWaitException):
    """Raised when a fatal error occurs"""

    def __init__(
        self,
        message: Any,
        elapsed: float,
        errors: Tuple[Exception, ...] = (),
    ):
        super().__init__(message, errors=errors)
        self.elapsed = elapsed

    def __repr__(self) -> str:
        parts = [repr(self.message)]
        if self.elapsed > 0:
            parts.append(f'elapsed={self.elapsed}')
        if self.errors:
            parts.append(f"errors={self.errors!r}")
        return f'{self.__class__.__name__}({", ".join(parts)})'


class EsWaitTimeout(Exception):
    """Raised when the timeout is reached"""

    def __init__(
        self,
        message: Any,
        elapsed: float,
        timeout: float,
    ):
        super().__init__(message)
        self.message = message
        self.elapsed = elapsed
        self.timeout = timeout

    def __repr__(self) -> str:
        parts = [repr(self.message)]
        if self.elapsed > 0.0:
            parts.append(f'elapsed={self.elapsed}')
        if self.timeout > 0.0:
            parts.append(f'timeout={self.timeout}')
        return f'{self.__class__.__name__}({", ".join(parts)})'


class ExceptionCount(EsWaitException):
    """Raised when the maximum number of exceptions is reached"""

    def __init__(
        self,
        message: Any,
        count: int,
        errors: Tuple[Exception, ...] = (),
    ):
        super().__init__(message, errors=errors)
        self.count = count

    def __repr__(self) -> str:
        parts = [repr(self.message)]
        if self.count > 0:
            parts.append(f'count={self.count}')
        if self.errors:
            parts.append(f"errors={self.errors!r}")
        return f'{self.__class__.__name__}({", ".join(parts)})'


class IlmWaitError(EsWaitException):
    """Any ILM-related Exception"""
