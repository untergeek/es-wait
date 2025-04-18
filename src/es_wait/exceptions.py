"""Custom exceptions for the es-wait package."""

from typing import Any, Tuple


class EsWaitException(Exception):
    """Base exception for es-wait package errors.

    Stores a message and related errors, ordered from most recent to least recent.

    Args:
        message (Any): The error message.
        errors (Tuple[:py:class:`Exception`, ...]): Related exceptions (default: ()).

    Attributes:
        message (Any): The error message.
        errors (Tuple[:py:class:`Exception`, ...]): Tuple of related exceptions.

    Example:
        >>> try:
        ...     raise ValueError("Invalid input")
        ... except ValueError as e:
        ...     raise EsWaitException("Operation failed", (e,))
    """

    def __init__(self, message: Any, errors: Tuple[Exception, ...] = ()):
        """Initialize the exception.

        Args:
            message (Any): The error message.
            errors (Tuple[:py:class:`Exception`, ...]): Related exceptions.

        Example:
            >>> exc = EsWaitException("Test error")
            >>> exc.message
            'Test error'
        """
        super().__init__(message)
        self.message = message
        self.errors = tuple(errors)

    def __repr__(self) -> str:
        """Return a string representation of the exception.

        Returns:
            str: String representation of the exception.

        Example:
            >>> exc = EsWaitException("Test error")
            >>> repr(exc)
            "EsWaitException('Test error')"
            >>> exc = EsWaitException("Test", (ValueError("Invalid"),))
            >>> repr(exc)
            "EsWaitException('Test', errors=(ValueError('Invalid'),))"
        """
        parts = [repr(self.message)]
        if self.errors:
            parts.append(f"errors={self.errors!r}")
        return f'{self.__class__.__name__}({", ".join(parts)})'

    def __str__(self) -> str:
        """Return the error message as a string.

        Returns:
            str: The error message.
        """
        return str(self.message)


class EsWaitFatal(EsWaitException):
    """Raised for fatal errors during waiting operations.

    Includes elapsed time and related errors.

    Args:
        message (Any): The error message.
        elapsed (float): Time elapsed during the operation.
        errors (Tuple[:py:class:`Exception`, ...]): Related exceptions (default: ()).

    Attributes:
        message (Any): The error message.
        elapsed (float): Time elapsed during the operation.
        errors (Tuple[:py:class:`Exception`, ...]): Tuple of related exceptions.

    Example:
        >>> raise EsWaitFatal("Fatal error", 10.0)
        Traceback (most recent call last):
            ...
        EsWaitFatal: Fatal error
    """

    def __init__(
        self,
        message: Any,
        elapsed: float,
        errors: Tuple[Exception, ...] = (),
    ):
        """Initialize the fatal exception.

        Args:
            message (Any): The error message.
            elapsed (float): Time elapsed during the operation.
            errors (Tuple[:py:class:`Exception`, ...]): Related exceptions.

        Example:
            >>> exc = EsWaitFatal("Fatal error", 10.0)
            >>> exc.elapsed
            10.0
        """
        super().__init__(message, errors=errors)
        self.elapsed = elapsed

    def __repr__(self) -> str:
        """Return a string representation of the exception.

        Returns:
            str: String representation of the exception.

        Example:
            >>> exc = EsWaitFatal("Fatal error", 10.0)
            >>> repr(exc)
            "EsWaitFatal('Fatal error', elapsed=10.0)"
            >>> exc = EsWaitFatal("Fatal", 5.0, (ValueError("Invalid"),))
            >>> repr(exc)
            "EsWaitFatal('Fatal', elapsed=5.0, errors=(ValueError('Invalid'),))"
        """
        parts = [repr(self.message)]
        if self.elapsed > 0:
            parts.append(f'elapsed={self.elapsed}')
        if self.errors:
            parts.append(f"errors={self.errors!r}")
        return f'{self.__class__.__name__}({", ".join(parts)})'


class EsWaitTimeout(Exception):
    """Raised when a waiting operation exceeds its timeout.

    Args:
        message (Any): The error message.
        elapsed (float): Time elapsed during the operation.
        timeout (float): Configured timeout limit.

    Attributes:
        message (Any): The error message.
        elapsed (float): Time elapsed during the operation.
        timeout (float): Configured timeout limit.

    Example:
        >>> raise EsWaitTimeout("Task timed out", 30.0, 30.0)
        Traceback (most recent call last):
            ...
        EsWaitTimeout: Task timed out
    """

    def __init__(
        self,
        message: Any,
        elapsed: float,
        timeout: float,
    ):
        """Initialize the timeout exception.

        Args:
            message (Any): The error message.
            elapsed (float): Time elapsed during the operation.
            timeout (float): Configured timeout limit.

        Example:
            >>> exc = EsWaitTimeout("Timeout", 30.0, 30.0)
            >>> exc.timeout
            30.0
        """
        super().__init__(message)
        self.message = message
        self.elapsed = elapsed
        self.timeout = timeout

    def __repr__(self) -> str:
        """Return a string representation of the exception.

        Returns:
            str: String representation of the exception.

        Example:
            >>> exc = EsWaitTimeout("Timeout", 30.0, 30.0)
            >>> repr(exc)
            "EsWaitTimeout('Timeout', elapsed=30.0, timeout=30.0)"
        """
        parts = [repr(self.message)]
        if self.elapsed > 0.0:
            parts.append(f'elapsed={self.elapsed}')
        if self.timeout > 0.0:
            parts.append(f'timeout={self.timeout}')
        return f'{self.__class__.__name__}({", ".join(parts)})'


class ExceptionCount(EsWaitException):
    """Raised when the maximum number of exceptions is reached.

    Args:
        message (Any): The error message.
        count (int): Number of exceptions encountered.
        errors (Tuple[:py:class:`Exception`, ...]): Related exceptions (default: ()).

    Attributes:
        message (Any): The error message.
        count (int): Number of exceptions encountered.
        errors (Tuple[:py:class:`Exception`, ...]): Tuple of related exceptions.

    Example:
        >>> raise ExceptionCount("Too many errors", 5)
        Traceback (most recent call last):
            ...
        ExceptionCount: Too many errors
    """

    def __init__(
        self,
        message: Any,
        count: int,
        errors: Tuple[Exception, ...] = (),
    ):
        """Initialize the exception count exception.

        Args:
            message (Any): The error message.
            count (int): Number of exceptions encountered.
            errors (Tuple[:py:class:`Exception`, ...]): Related exceptions.

        Example:
            >>> exc = ExceptionCount("Too many errors", 5)
            >>> exc.count
            5
        """
        super().__init__(message, errors=errors)
        self.count = count

    def __repr__(self) -> str:
        """Return a string representation of the exception.

        Returns:
            str: String representation of the exception.

        Example:
            >>> exc = ExceptionCount("Too many errors", 5)
            >>> repr(exc)
            "ExceptionCount('Too many errors', count=5)"
            >>> exc = ExceptionCount("Errors", 3, (ValueError("Invalid"),))
            >>> repr(exc)
            "ExceptionCount('Errors', count=3, errors=(ValueError('Invalid'),))"
        """
        parts = [repr(self.message)]
        if self.count > 0:
            parts.append(f'count={self.count}')
        if self.errors:
            parts.append(f"errors={self.errors!r}")
        return f'{self.__class__.__name__}({", ".join(parts)})'


class IlmWaitError(EsWaitException):
    """Raised for ILM-related errors during waiting operations.

    Args:
        message (Any): The error message.
        errors (Tuple[:py:class:`Exception`, ...]): Related exceptions (default: ()).

    Attributes:
        message (Any): The error message.
        errors (Tuple[:py:class:`Exception`, ...]): Tuple of related exceptions.

    Example:
        >>> raise IlmWaitError("ILM phase stuck")
        Traceback (most recent call last):
            ...
        IlmWaitError: ILM phase stuck
    """

    def __repr__(self) -> str:
        """Return a string representation of the exception.

        Returns:
            str: String representation of the exception.

        Example:
            >>> exc = IlmWaitError("ILM error")
            >>> repr(exc)
            "IlmWaitError('ILM error')"
            >>> exc = IlmWaitError("ILM", (ValueError("Invalid"),))
            >>> repr(exc)
            "IlmWaitError('ILM', errors=(ValueError('Invalid'),))"
        """
        parts = [repr(self.message)]
        if self.errors:
            parts.append(f"errors={self.errors!r}")
        return f'{self.__class__.__name__}({", ".join(parts)})'
