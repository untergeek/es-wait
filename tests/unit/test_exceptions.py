"""Test exceptions module."""

from es_wait.exceptions import EsWaitFatal, EsWaitTimeout, ExceptionCount


def test_eswaitfatal_init():
    """Test EsWaitFatal initialization and attributes."""
    errors = [Exception("error1"), Exception("error2")]
    exc = EsWaitFatal("Fatal error", elapsed=10.0, errors=errors)
    assert exc.message == "Fatal error"
    assert exc.elapsed == 10.0
    assert exc.errors == tuple(errors)
    assert str(exc) == "Fatal error"


def test_eswaittimeout_init():
    """Test EsWaitTimeout initialization with defaults."""
    msg = "Timeout occurred after 5.0s"
    exc = EsWaitTimeout(msg, 5.0, 5.0)
    assert exc.elapsed == 5.0
    assert str(exc) == "Timeout occurred after 5.0s"


def test_exceptioncount_init():
    """Test ExceptionCount initialization and string representation."""
    msg = "Exception count 3 exceeded threshold after 2.0s"
    exc = ExceptionCount(msg, 3)
    assert exc.message == msg
    assert exc.count == 3
