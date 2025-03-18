"""Unit tests for Task"""

# pylint: disable=W0212, W0621
import logging
from unittest.mock import MagicMock, patch
import time
import pytest
from elasticsearch8.exceptions import TransportError
from es_wait._base import TimeTracker, Waiter
from es_wait.exceptions import (
    EsWaitException,
    EsWaitFatal,
    EsWaitTimeout,
    ExceptionCount,
    IlmWaitError,
)

# --- TimeTracker Tests ---


def test_timetracker_should_log():
    """Test that should_log returns True at specified intervals."""
    tracker = TimeTracker(log_frequency=2)
    count = 0
    for _ in range(0, 7):
        if int(tracker.elapsed) == 0:
            assert tracker.should_log is False
        elif int(tracker.elapsed) % 2 == 0:
            assert tracker.should_log is True
            count += 1
        else:
            assert tracker.should_log is False
        time.sleep(1)
    assert count == 3  # Should log every 2 seconds


# Waiter Tests
@pytest.fixture
def waiter():
    """Create a basic Waiter instance with a mocked client."""
    client = MagicMock()
    return Waiter(client, pause=0.5, timeout=2.0, max_exceptions=3)


def test_waiter_init(waiter):
    """Test Waiter initialization with custom parameters."""
    assert waiter.pause == 0.5
    assert waiter.timeout == 2.0
    assert waiter.max_exceptions == 3
    assert waiter.exceptions_raised == 0


def test_waiter_check_default(waiter):
    """Test that the default check method returns False."""
    assert not waiter.check()


def test_waiter_wait_success(waiter):
    """Test wait exits immediately when check returns True."""
    waiter.check = MagicMock(return_value=True)
    with patch('es_wait._base.sleep') as mock_sleep:
        waiter.wait()
        mock_sleep.assert_not_called()  # No sleep if check succeeds


def test_waiter_timeout(waiter):
    """Test that EsWaitTimeout is raised when timeout is exceeded."""
    waiter.check = MagicMock(return_value=False)
    with pytest.raises(EsWaitTimeout) as exc_info:
        waiter.wait()
    assert 2.7 > exc_info.value.elapsed > 1.9  # Timeout occurred
    assert exc_info.value.timeout == 2.0  # Confirm that the timeout is 2.0


def test_waiter_max_exceptions(waiter):
    """Test that EsWaitFatal is raised when max_exceptions is exceeded."""
    exc = ExceptionCount("Test error", 3)
    waiter.exceptions_raised = 3
    waiter.check = MagicMock(side_effect=exc)
    with pytest.raises(EsWaitFatal) as exc_info:
        waiter.wait()
    assert str(exc_info.value.message) == "3 exceptions raised out of 3 allowed"


def test_health_report_logic(waiter, caplog):
    """
    Test the health report logic in Waiter.wait():
    - Skips health report when do_health_report is False.
    - Calls health_report when do_health_report is True and
        client.health_report() succeeds.
    - Logs an error and continues when client.health_report() raises TransportError.
    """

    # Set logging level to capture ERROR messages
    caplog.set_level("ERROR")

    # Initialize Waiter with short timeout and pause
    waiter.check = MagicMock(return_value=False)  # Force timeout

    # Scenario 1: do_health_report=False
    waiter.do_health_report = False
    with pytest.raises(EsWaitTimeout):
        waiter.wait()
    assert waiter.client.health_report.call_count == 0  # No calls made
    assert len(caplog.records) == 2  # No logs generated

    # Scenario 2: do_health_report=True, successful health report
    waiter.do_health_report = True
    waiter.client.health_report.return_value = {"status": "green"}  # Mock success
    with pytest.raises(EsWaitTimeout):
        waiter.wait()
    assert waiter.client.health_report.call_count == 1  # Called once

    # Scenario 3: do_health_report=True, TransportError raised
    waiter.client.health_report.side_effect = TransportError("Connection failed")
    with pytest.raises(EsWaitTimeout):
        waiter.wait()
    assert waiter.client.health_report.call_count == 2  # Called again
    assert "Health report failed: Connection failed" in caplog.text  # Error logged


def test_waiter_es_wait_exception(waiter, caplog):
    """Test that EsWaitFatal is raised and logged when EsWaitException is caught."""
    caplog.set_level(logging.WARNING)
    waiter.check = MagicMock(side_effect=EsWaitException("Test EsWaitException"))
    with pytest.raises(EsWaitFatal) as exc_info:
        waiter.wait()
    assert "An error occurred:" in caplog.text
    assert "EsWaitException('Test EsWaitException')" in caplog.text
    assert isinstance(exc_info.value, EsWaitFatal)


def test_waiter_ilm_wait_error(waiter, caplog):
    """Test that EsWaitFatal is raised and logged when IlmWaitError is caught."""
    caplog.set_level(logging.WARNING)
    waiter.check = MagicMock(side_effect=IlmWaitError("Test IlmWaitError"))
    with pytest.raises(EsWaitFatal) as exc_info:
        waiter.wait()
    assert "An error occurred" in caplog.text
    assert "IlmWaitError('Test IlmWaitError')" in caplog.text
    assert isinstance(exc_info.value, EsWaitFatal)
